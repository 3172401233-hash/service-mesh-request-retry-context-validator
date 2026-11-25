"""Retry context validation across gateway and downstream services.

The validator consumes the seven inputs named in the task:

* the original request (used to recompute wire/canonical body digests),
* the request body digest claimed at the entry,
* the idempotency key,
* the ordered call chain (hops and their attempts),
* the retry count / budget,
* the attempt id,
* the response status.

It decides for every chain position whether a retry is:

* ``retryable``               - safe direct replay (same key + canonical digest,
                                budget left, retryable status, no effects);
* ``idempotent_retry_only``   - a committed effect exists; direct replay is
                                forbidden, retry solely through key dedup;
* ``blocked``                 - must not be retried automatically;
* ``not_required``            - last attempt succeeded;
* ``no_attempt_recorded``     - nothing to retry yet.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from .cancellation import propagate_cancellation
from .idempotency import generate_idempotency_key
from .model import (
    Attempt,
    Position,
    Scenario,
    is_retryable_status,
    is_success_status,
    scenario_from_dict,
)


DECISION_RETRYABLE = "retryable"
DECISION_GUARDED = "idempotent_retry_only"
DECISION_BLOCKED = "blocked"
DECISION_NOT_REQUIRED = "not_required"
DECISION_NO_ATTEMPT = "no_attempt_recorded"


class RetryContextValidator:
    """Validate one logical request's retry context."""

    def __init__(
        self,
        key_generator: Callable[[], str] = generate_idempotency_key,
    ) -> None:
        self._key_generator = key_generator

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def validate(self, scenario: Any) -> Dict[str, Any]:
        sc = scenario_from_dict(scenario)
        findings: List[Dict[str, Any]] = []

        request = sc.original_request
        request_provided = request is not None
        if request_provided:
            wire_digest = request.wire_digest()
            canonical_digest = request.body_digest()
        else:
            # No raw request to re-hash: trust the supplied digest as the
            # canonical binding value for cross-attempt comparisons.
            wire_digest = None
            canonical_digest = sc.body_digest

        digest_report = self._build_digest_report(
            sc,
            wire_digest,
            canonical_digest,
            request_provided,
            findings,
        )

        key, key_source = self._resolve_key(sc, findings)

        positions = [
            self._base_position_decision(
                i, p, sc, key, canonical_digest, wire_digest, findings
            )
            for i, p in enumerate(sc.call_chain)
        ]

        effects = self._collect_effects(sc, key)
        self._apply_effect_rules(positions, sc, effects, findings)

        cancellation_report = self._handle_cancellation(sc, positions, findings)

        risks = self._build_risks(sc, positions, effects, key, findings)

        retryable_positions = [
            self._retryable_entry(pd)
            for pd in positions
            if pd["decision"] in (DECISION_RETRYABLE, DECISION_GUARDED)
        ]

        evaluated_attempt = self._evaluate_named_attempt(sc, positions)

        high_risks = [r for r in risks if r["risk_level"] == "high"]
        report = {
            "idempotency_key": key,
            "idempotency_key_source": key_source,
            "canonical_body_digest": canonical_digest,
            "wire_body_digest": wire_digest,
            "original_request_provided": request_provided,
            "digest": digest_report,
            "call_chain_length": len(sc.call_chain),
            "positions": positions,
            "retryable_positions": retryable_positions,
            "duplicate_side_effect_risks": risks,
            "cancellation": cancellation_report,
            "findings": findings,
            "evaluated_attempt": evaluated_attempt,
            "summary": {
                "position_count": len(positions),
                "retryable_position_count": len(retryable_positions),
                "direct_retryable_count": sum(
                    1 for item in retryable_positions if item["mode"] == "direct"
                ),
                "guarded_retry_count": sum(
                    1
                    for item in retryable_positions
                    if item["mode"] == "idempotent_dedup"
                ),
                "high_duplicate_side_effect_risk_count": len(high_risks),
                "cancellation_propagated_to_all": cancellation_report.get(
                    "propagated_to_all", True
                ),
            },
        }
        return report

    # ------------------------------------------------------------------
    # Digest / key resolution
    # ------------------------------------------------------------------
    def _build_digest_report(
        self,
        sc: Scenario,
        wire_digest: Optional[str],
        canonical_digest: Optional[str],
        request_provided: bool,
        findings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if request_provided:
            encoding = (sc.original_request.content_encoding or "identity").lower()
        else:
            encoding = "unknown"
        decompressed = encoding not in ("", "identity", "none")
        changed = bool(
            wire_digest is not None
            and canonical_digest is not None
            and wire_digest != canonical_digest
        )

        claimed = sc.body_digest
        claimed_matches = None
        if claimed and request_provided:
            if sc.body_digest_basis == "wire":
                claimed_matches = claimed == wire_digest
                if not claimed_matches:
                    findings.append(
                        _finding(
                            "CLAIMED_WIRE_DIGEST_MISMATCH",
                            "error",
                            "provided wire body digest does not match raw bytes",
                        )
                    )
            else:
                claimed_matches = claimed == canonical_digest
                if not claimed_matches:
                    findings.append(
                        _finding(
                            "CLAIMED_BODY_DIGEST_MISMATCH",
                            "error",
                            "provided body digest does not match canonical "
                            "(decoded) request body",
                            expected=canonical_digest,
                            actual=claimed,
                        )
                    )
        elif claimed and not request_provided:
            claimed_matches = None

        if request_provided and decompressed and changed:
            findings.append(
                _finding(
                    "DIGEST_CHANGED_AFTER_DECOMPRESSION",
                    "info",
                    "wire digest differs from canonical digest because the "
                    "first hop decompressed the body; idempotency binding must "
                    "use the canonical digest, not the compressed wire digest",
                    wire_digest=wire_digest,
                    canonical_digest=canonical_digest,
                    content_encoding=encoding,
                )
            )

        # The dangerous mistake: binding/reusing the original wire digest
        # after the body has been decompressed changes the comparison value.
        stale_wire_binding = bool(
            claimed
            and sc.body_digest_basis == "canonical"
            and claimed == wire_digest
            and wire_digest != canonical_digest
        )
        if stale_wire_binding:
            findings.append(
                _finding(
                    "STALE_WIRE_DIGEST_BINDING",
                    "error",
                    "entry bound the idempotency record to the compressed wire "
                    "digest; after the first hop decompresses, every retry "
                    "digest looks different and result reuse / conflict checks "
                    "become unreliable",
                )
            )

        return {
            "content_encoding": encoding,
            "original_request_provided": request_provided,
            "decompression_applied": decompressed,
            "wire_digest": wire_digest,
            "canonical_digest": canonical_digest,
            "digest_changed_after_decompression": bool(
                request_provided and changed and decompressed
            ),
            "claimed_digest": claimed,
            "claimed_digest_basis": sc.body_digest_basis,
            "claimed_digest_matches": claimed_matches,
            "stale_wire_digest_binding": stale_wire_binding,
        }

    def _resolve_key(
        self, sc: Scenario, findings: List[Dict[str, Any]]
    ) -> Tuple[str, str]:
        if sc.idempotency_key:
            return sc.idempotency_key, "provided"
        key = self._key_generator()
        findings.append(
            _finding(
                "IDEMPOTENCY_KEY_GENERATED_AT_ENTRY",
                "info",
                "no key was supplied; generated one at the entry point only",
                idempotency_key=key,
            )
        )
        return key, "generated_at_entry"

    # ------------------------------------------------------------------
    # Per-position base decisions (status / budget / key / digest)
    # ------------------------------------------------------------------
    def _base_position_decision(
        self,
        index: int,
        position: Position,
        sc: Scenario,
        key: str,
        canonical_digest: Optional[str],
        wire_digest: Optional[str],
        findings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        budget = position.retry_budget(sc.retry_count)
        used = position.retries_used()
        last = position.attempts[-1] if position.attempts else None

        reason_codes: List[str] = []
        blockers: List[str] = []

        if position.generated_key_here and index > 0:
            blockers.append("KEY_GENERATED_DOWNSTREAM")
            findings.append(
                _finding(
                    "KEY_GENERATED_DOWNSTREAM",
                    "error",
                    "position %r mints an idempotency key; only the entry may "
                    "generate and downstream hops must propagate it"
                    % position.name,
                    position=position.name,
                )
            )

        self._check_key_and_digests(
            position,
            key,
            canonical_digest,
            wire_digest,
            reason_codes,
            blockers,
            findings,
        )

        if last is None:
            decision = DECISION_NO_ATTEMPT
            retry_allowed = False
            direct_allowed = False
        else:
            budget_exhausted = used >= budget
            if budget_exhausted:
                blockers.append("RETRY_BUDGET_EXHAUSTED")
                reason_codes.append("RETRY_BUDGET_EXHAUSTED")

            if is_success_status(last.response_status):
                status_decision = DECISION_NOT_REQUIRED
            elif not is_retryable_status(last.response_status):
                status_decision = DECISION_BLOCKED
                blockers.append("STATUS_NOT_RETRYABLE")
                reason_codes.append("STATUS_NOT_RETRYABLE")
            else:
                status_decision = DECISION_RETRYABLE
                reason_codes.append("STATUS_ALLOWS_RETRY")
                if last.response_status is None:
                    reason_codes.append("RESPONSE_OUTCOME_UNKNOWN")

            context_blocked = any(
                code in blockers
                for code in (
                    "KEY_MISSING",
                    "KEY_MISMATCH",
                    "KEY_GENERATED_DOWNSTREAM",
                    "BODY_DIGEST_MISMATCH",
                )
            )

            if status_decision == DECISION_NOT_REQUIRED and not context_blocked:
                decision = DECISION_NOT_REQUIRED
                retry_allowed = False
                direct_allowed = False
            elif context_blocked:
                decision = DECISION_BLOCKED
                retry_allowed = False
                direct_allowed = False
            elif "STATUS_NOT_RETRYABLE" in blockers:
                decision = DECISION_BLOCKED
                retry_allowed = False
                direct_allowed = False
            elif budget_exhausted:
                decision = DECISION_BLOCKED
                retry_allowed = False
                direct_allowed = False
            else:
                decision = DECISION_RETRYABLE
                retry_allowed = True
                direct_allowed = True

        reason_codes = list(dict.fromkeys(reason_codes))
        blockers = list(dict.fromkeys(blockers))

        next_retry_number = used + 1
        suggested_attempt_id = None
        if last is not None and decision in (
            DECISION_RETRYABLE,
            DECISION_GUARDED,
        ):
            suggested_attempt_id = "%s-r%d" % (last.attempt_id, next_retry_number)

        return {
            "position": position.name,
            "position_index": index,
            "caller": position.caller,
            "callee": position.callee,
            "decision": decision,
            "retry_allowed": retry_allowed,
            "direct_replay_allowed": direct_allowed,
            "guard_required": False,
            "retries_used": used,
            "retry_budget": budget,
            "next_retry_count": next_retry_number,
            "suggested_attempt_id": suggested_attempt_id,
            "last_attempt_id": last.attempt_id if last else None,
            "last_response_status": last.response_status if last else None,
            "reason_codes": reason_codes,
            "blockers": blockers,
        }

    def _check_key_and_digests(
        self,
        position: Position,
        key: str,
        canonical: Optional[str],
        wire: Optional[str],
        reason_codes: List[str],
        blockers: List[str],
        findings: List[Dict[str, Any]],
    ) -> None:
        position_key = position.idempotency_key
        for attempt in position.attempts:
            observed_key = attempt.idempotency_key or position_key
            if observed_key is None:
                blockers.append("KEY_MISSING")
                reason_codes.append("KEY_MISSING")
                findings.append(
                    _finding(
                        "IDEMPOTENCY_KEY_MISSING",
                        "error",
                        "attempt %s at %r does not carry the entry idempotency "
                        "key; downstream dedup cannot identify the logical request"
                        % (attempt.attempt_id, position.name),
                        position=position.name,
                        attempt_id=attempt.attempt_id,
                    )
                )
            elif observed_key != key:
                blockers.append("KEY_MISMATCH")
                reason_codes.append("KEY_MISMATCH")
                findings.append(
                    _finding(
                        "IDEMPOTENCY_KEY_MISMATCH",
                        "error",
                        "attempt %s at %r uses a different idempotency key"
                        % (attempt.attempt_id, position.name),
                        position=position.name,
                        attempt_id=attempt.attempt_id,
                        expected=key,
                        actual=observed_key,
                    )
                )

            if attempt.generated_key_here:
                findings.append(
                    _finding(
                        "KEY_GENERATED_DOWNSTREAM",
                        "error",
                        "attempt %s at %r generated a new key; only the entry "
                        "may generate one" % (attempt.attempt_id, position.name),
                        position=position.name,
                        attempt_id=attempt.attempt_id,
                    )
                )
                blockers.append("KEY_GENERATED_DOWNSTREAM")

            digest = attempt.body_digest
            if not digest:
                reason_codes.append("ATTEMPT_DIGEST_UNREPORTED")
                findings.append(
                    _finding(
                        "ATTEMPT_DIGEST_UNREPORTED",
                        "warning",
                        "attempt %s at %r reports no body digest; body binding "
                        "cannot be verified"
                        % (attempt.attempt_id, position.name),
                        position=position.name,
                        attempt_id=attempt.attempt_id,
                    )
                )
                continue

            if attempt.digest_basis == "wire":
                expected = wire
                basis_note = "wire"
            else:
                expected = canonical
                basis_note = "canonical"

            if expected is None:
                reason_codes.append("ATTEMPT_DIGEST_UNVERIFIABLE")
                findings.append(
                    _finding(
                        "ATTEMPT_DIGEST_UNVERIFIABLE",
                        "warning",
                        "attempt %s at %r reports a %s digest but neither the "
                        "original request nor an entry body_digest is "
                        "available to compare it against"
                        % (attempt.attempt_id, position.name, basis_note),
                        position=position.name,
                        attempt_id=attempt.attempt_id,
                    )
                )
                continue

            if digest != expected:
                blockers.append("BODY_DIGEST_MISMATCH")
                reason_codes.append("BODY_DIGEST_MISMATCH")
                findings.append(
                    _finding(
                        "BODY_DIGEST_MISMATCH",
                        "error",
                        "attempt %s at %r carries a %s digest that does not "
                        "match the logical request; a different body must not "
                        "reuse the idempotent result"
                        % (attempt.attempt_id, position.name, basis_note),
                        position=position.name,
                        attempt_id=attempt.attempt_id,
                        expected=expected,
                        actual=digest,
                    )
                )

    # ------------------------------------------------------------------
    # Side effects
    # ------------------------------------------------------------------
    def _collect_effects(self, sc: Scenario, key: str) -> List[Dict[str, Any]]:
        effects: List[Dict[str, Any]] = []
        for j, position in enumerate(sc.call_chain):
            for attempt in position.attempts:
                for effect in attempt.side_effects:
                    if not effect.committed:
                        continue
                    dedup_key = effect.dedup_key
                    key_bound = effect.idempotency_protected and (
                        dedup_key is None or dedup_key == key
                    )
                    effects.append(
                        {
                            "position_index": j,
                            "position": position.name,
                            "attempt_id": attempt.attempt_id,
                            "attempt": attempt,
                            "name": effect.name,
                            "protected": key_bound,
                            "claimed_protected": effect.idempotency_protected,
                            "dedup_key": dedup_key or key,
                            "branch_id": effect.branch_id,
                            "detail": effect.detail,
                        }
                    )
            # Branch-declared effects are handled in cancellation report but
            # also participate in chain risk when tied to a position later.
        return effects

    def _apply_effect_rules(
        self,
        positions: List[Dict[str, Any]],
        sc: Scenario,
        effects: List[Dict[str, Any]],
        findings: List[Dict[str, Any]],
    ) -> None:
        for pd in positions:
            i = pd["position_index"]
            relevant = [e for e in effects if e["position_index"] >= i]
            if not relevant:
                continue

            unprotected = [e for e in relevant if not e["protected"]]
            protected = [e for e in relevant if e["protected"]]

            if pd["decision"] == DECISION_NO_ATTEMPT:
                continue
            if pd["decision"] == DECISION_NOT_REQUIRED:
                # This edge itself succeeded; upstream positions still inherit
                # the effect via their own relevant sets.
                continue
            if pd["decision"] == DECISION_BLOCKED and (
                "KEY_MISMATCH" in pd["blockers"]
                or "KEY_MISSING" in pd["blockers"]
                or "BODY_DIGEST_MISMATCH" in pd["blockers"]
                or "RETRY_BUDGET_EXHAUSTED" in pd["blockers"]
                or "CANCELLATION_REQUESTED" in pd["blockers"]
            ):
                continue

            if unprotected:
                names = sorted({e["name"] for e in unprotected})
                downstream = any(e["position_index"] > i for e in unprotected)
                code = (
                    "DOWNSTREAM_UNPROTECTED_EFFECT_WOULD_REPLAY"
                    if downstream
                    else "UNPROTECTED_SIDE_EFFECT_NO_DIRECT_REPLAY"
                )
                pd["decision"] = DECISION_BLOCKED
                pd["retry_allowed"] = False
                pd["direct_replay_allowed"] = False
                pd["guard_required"] = False
                if code not in pd["reason_codes"]:
                    pd["reason_codes"].append(code)
                if code not in pd["blockers"]:
                    pd["blockers"].append(code)
                findings.append(
                    _finding(
                        code,
                        "error",
                        "position %r cannot directly retry: committed "
                        "unprotected side effect(s) %s would be executed again"
                        % (pd["position"], ", ".join(names)),
                        position=pd["position"],
                        side_effects=names,
                    )
                )
            elif protected and pd["decision"] == DECISION_RETRYABLE:
                pd["decision"] = DECISION_GUARDED
                pd["retry_allowed"] = True
                pd["direct_replay_allowed"] = False
                pd["guard_required"] = True
                code = "IDEMPOTENT_DEDUP_REQUIRED"
                pd["reason_codes"].append(code)
                findings.append(
                    _finding(
                        code,
                        "warning",
                        "position %r may only retry with the shared "
                        "idempotency key so the effect owner deduplicates"
                        % pd["position"],
                        position=pd["position"],
                    )
                )

    def _build_risks(
        self,
        sc: Scenario,
        positions: List[Dict[str, Any]],
        effects: List[Dict[str, Any]],
        key: str,
        findings: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        risks: List[Dict[str, Any]] = []
        for effect in effects:
            j = effect["position_index"]
            owner_attempt: Attempt = effect["attempt"]

            trigger_positions = [
                positions[k]["position"]
                for k in range(0, j + 1)
                if k < len(positions)
            ]
            direct_triggers = [
                p["position"]
                for p in positions[: j + 1]
                if p["decision"] == DECISION_RETRYABLE
            ]
            guarded_triggers = [
                p["position"]
                for p in positions[: j + 1]
                if p["decision"] == DECISION_GUARDED
            ]

            ambiguous = owner_attempt.outcome_unknown()
            upstream_uncertain = self._upstream_outcome_uncertain(positions, j)

            if effect["protected"]:
                level = "low"
                action = (
                    "retry only with the same idempotency key and confirm the "
                    "dedup store is shared by every hop"
                )
                code = "PROTECTED_DUPLICATE_SIDE_EFFECT"
            elif ambiguous or upstream_uncertain or direct_triggers:
                level = "high"
                action = (
                    "do not replay directly; query/reconcile the effect and "
                    "retry only after installing idempotent dedup"
                )
                code = "DUPLICATE_SIDE_EFFECT_RISK"
            else:
                level = "medium"
                action = (
                    "effect is known to have completed; avoid retries and add "
                    "idempotent dedup before any timeout-driven retry"
                )
                code = "POTENTIAL_DUPLICATE_SIDE_EFFECT"

            risk = {
                "code": code,
                "risk_level": level,
                "position": effect["position"],
                "attempt_id": effect["attempt_id"],
                "side_effect": effect["name"],
                "branch_id": effect["branch_id"],
                "idempotency_key": effect["dedup_key"],
                "idempotency_protected": effect["protected"],
                "outcome_unknown": ambiguous,
                "upstream_outcome_uncertain": upstream_uncertain,
                "would_retrigger_positions": trigger_positions,
                "currently_direct_retryable_positions": direct_triggers,
                "currently_guarded_retry_positions": guarded_triggers,
                "required_action": action,
            }
            if effect["claimed_protected"] and not effect["protected"]:
                risk["risk_level"] = "high"
                risk["code"] = "DEDUP_KEY_SCOPE_MISMATCH"
                risk["detail"] = (
                    "effect claims idempotency protection but its dedup key "
                    "does not match the logical request key"
                )
            risks.append(risk)

        for branch_risk in getattr(self, "_branch_risks", []):
            risks.append(
                {
                    "code": branch_risk.get("code", "CANCELLATION_BRANCH_RISK"),
                    "risk_level": branch_risk.get(
                        "risk_level", branch_risk.get("severity", "medium")
                    ),
                    "position": branch_risk.get("position"),
                    "attempt_id": branch_risk.get("attempt_id"),
                    "side_effect": branch_risk.get("side_effect"),
                    "branch_id": branch_risk.get("branch_id"),
                    "idempotency_key": key,
                    "idempotency_protected": branch_risk.get(
                        "idempotency_protected", False
                    ),
                    "outcome_unknown": True,
                    "would_retrigger_positions": [],
                    "currently_direct_retryable_positions": [],
                    "currently_guarded_retry_positions": [],
                    "required_action": branch_risk.get("required_action", ""),
                    "detail": branch_risk.get("detail", ""),
                }
            )

        high = [r for r in risks if r["risk_level"] == "high"]
        if high:
            findings.append(
                _finding(
                    "DUPLICATE_SIDE_EFFECT_RISK",
                    "error",
                    "%d high-severity duplicate side-effect risk(s) detected"
                    % len(high),
                )
            )
        return risks

    def _upstream_outcome_uncertain(
        self, positions: List[Dict[str, Any]], j: int
    ) -> bool:
        for pd in positions[:j]:
            status = pd["last_response_status"]
            if status is None or is_retryable_status(status):
                if not is_success_status(status):
                    return True
        return False

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------
    def _handle_cancellation(
        self,
        sc: Scenario,
        positions: List[Dict[str, Any]],
        findings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        cancel_cfg = sc.cancellation or {}
        requested = bool(cancel_cfg.get("requested", cancel_cfg.get("cancelled", False)))
        explicit_reached = cancel_cfg.get("reached", cancel_cfg.get("propagated_to"))

        report = propagate_cancellation(
            sc.branches,
            requested=requested,
            explicit_reached=list(explicit_reached) if explicit_reached else None,
        )

        chain_targets = [p.name for p in sc.call_chain]
        report["chain_targets"] = chain_targets
        if requested:
            report["targets"] = chain_targets + report["targets"]

        self._branch_risks = report.get("risks", [])

        if requested:
            for pd in positions:
                if pd["decision"] in (
                    DECISION_RETRYABLE,
                    DECISION_GUARDED,
                ) or pd["decision"] == DECISION_BLOCKED:
                    if "CANCELLATION_REQUESTED" not in pd["blockers"]:
                        pd["blockers"].append("CANCELLATION_REQUESTED")
                    if "CANCELLATION_REQUESTED" not in pd["reason_codes"]:
                        pd["reason_codes"].append("CANCELLATION_REQUESTED")
                    pd["decision"] = DECISION_BLOCKED
                    pd["retry_allowed"] = False
                    pd["direct_replay_allowed"] = False
                    pd["suggested_attempt_id"] = None
            findings.append(
                _finding(
                    "CANCELLATION_REQUESTED",
                    "warning",
                    "cancellation requested; no hop may start a new retry",
                    targets=report["targets"],
                )
            )

        if report.get("late_arrivals"):
            findings.append(
                _finding(
                    "CANCELLATION_ARRIVED_LATE",
                    "error",
                    "cancellation arrived after %d branch(es) committed work"
                    % len(report["late_arrivals"]),
                    late=[item["branch_id"] for item in report["late_arrivals"]],
                )
            )

        if requested and not report.get("propagated_to_all", True):
            findings.append(
                _finding(
                    "CANCELLATION_NOT_PROPAGATED_TO_ALL_BRANCHES",
                    "error",
                    "cancellation did not reach every branch",
                    not_propagated=report.get("not_propagated", []),
                )
            )

        return report

    # ------------------------------------------------------------------
    # Selected attempt / output shaping
    # ------------------------------------------------------------------
    def _evaluate_named_attempt(
        self, sc: Scenario, positions: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        attempt_id = sc.attempt_id
        if not attempt_id:
            if sc.response_status is not None or not sc.call_chain:
                return {
                    "attempt_id": None,
                    "response_status": sc.response_status,
                    "note": "pass attempt_id to locate a specific attempt",
                }
            return None

        for p, pd in zip(sc.call_chain, positions):
            for attempt in p.attempts:
                if attempt.attempt_id == attempt_id:
                    return {
                        "attempt_id": attempt_id,
                        "response_status": attempt.response_status,
                        "position": p.name,
                        "position_decision": pd["decision"],
                        "retry_allowed": pd["retry_allowed"],
                        "direct_replay_allowed": pd["direct_replay_allowed"],
                        "reason_codes": list(pd["reason_codes"]),
                    }
        return {
            "attempt_id": attempt_id,
            "found": False,
            "note": "attempt_id not present in call_chain",
        }

    @staticmethod
    def _retryable_entry(pd: Dict[str, Any]) -> Dict[str, Any]:
        guarded = pd["decision"] == DECISION_GUARDED
        return {
            "position": pd["position"],
            "position_index": pd["position_index"],
            "mode": "idempotent_dedup" if guarded else "direct",
            "decision": pd["decision"],
            "direct_replay_allowed": pd["direct_replay_allowed"],
            "guard_required": pd["guard_required"],
            "suggested_attempt_id": pd["suggested_attempt_id"],
            "next_retry_count": pd["next_retry_count"],
            "reason_codes": list(pd["reason_codes"]),
        }


def _finding(code: str, severity: str, message: str, **extra: Any) -> Dict[str, Any]:
    item = {"code": code, "severity": severity, "message": message}
    item.update(extra)
    return item


# ----------------------------------------------------------------------
# Module-level convenience API matching the seven requested inputs
# ----------------------------------------------------------------------
def evaluate_retry(
    original_request: Any = None,
    request_body_digest: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    call_chain: Any = None,
    retry_count: int = 2,
    attempt_id: Optional[str] = None,
    response_status: Optional[int] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Validate retry context from the seven named inputs.

    When ``call_chain`` is empty but ``attempt_id`` is supplied, a single-hop
    chain is synthesized so a lone attempt id + status can be evaluated.
    Additional keyword arguments (``branches``, ``cancellation``,
    ``side_effects`` ...) are forwarded into the scenario.
    """
    chain = list(call_chain or [])
    contains_attempt = _chain_contains_attempt(chain, attempt_id)

    if not chain and attempt_id is not None:
        chain = [
            {
                "name": kwargs.pop("position_name", "entry->downstream"),
                "attempts": [
                    {
                        "attempt_id": attempt_id,
                        "response_status": response_status,
                        "retry_count": kwargs.pop("attempt_retry_count", 0),
                        "idempotency_key": idempotency_key,
                        "body_digest": request_body_digest,
                        "side_effects": kwargs.pop("side_effects", []),
                    }
                ],
            }
        ]
    elif attempt_id is not None and not contains_attempt and response_status is not None:
        # Append the described attempt to the last position for evaluation.
        if chain and isinstance(chain[-1], dict):
            chain[-1].setdefault("attempts", []).append(
                {
                    "attempt_id": attempt_id,
                    "response_status": response_status,
                    "idempotency_key": idempotency_key,
                    "body_digest": request_body_digest,
                }
            )

    scenario: Dict[str, Any] = {
        "original_request": original_request,
        "body_digest": request_body_digest,
        "idempotency_key": idempotency_key,
        "retry_count": retry_count,
        "call_chain": chain,
        "attempt_id": attempt_id,
        "response_status": response_status,
    }
    scenario.update(kwargs)
    return RetryContextValidator().validate(scenario)


def _chain_contains_attempt(chain: Any, attempt_id: Optional[str]) -> bool:
    if not attempt_id:
        return False
    for position in chain:
        attempts = None
        if isinstance(position, dict):
            attempts = position.get("attempts")
        else:
            attempts = getattr(position, "attempts", None)
        if not attempts:
            continue
        for attempt in attempts:
            aid = attempt.get("attempt_id") if isinstance(attempt, dict) else attempt.attempt_id
            if aid == attempt_id:
                return True
    return False


def validate_scenario(scenario: Any) -> Dict[str, Any]:
    return RetryContextValidator().validate(scenario)
