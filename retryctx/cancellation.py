"""Cancellation propagation across fan-out branches.

A cancellation must reach EVERY branch.  If it arrives after a branch has
already committed a side effect, the signal is "late" for that branch: the
effect cannot be unsent, but the late arrival is recorded as a duplicate /
compensation risk while all still-running branches are cancelled.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .model import Branch


def _effect_dicts(branch: Branch) -> List[Dict[str, Any]]:
    return [
        {
            "name": effect.name,
            "committed": effect.committed,
            "idempotency_protected": effect.idempotency_protected,
            "dedup_key": effect.dedup_key,
        }
        for effect in branch.side_effects
        if effect.committed
    ]


def propagate_cancellation(
    branches: List[Branch],
    requested: bool = True,
    explicit_reached: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Propagate a cancellation signal to all branches.

    The branch objects are mutated to reflect the simulated propagation
    (running branches become ``cancelled``).  Returns a JSON-friendly report.
    """
    all_targets = [branch.branch_id for branch in branches]

    if not requested:
        return {
            "requested": False,
            "propagated_to_all": False if all_targets else True,
            "targets": all_targets,
            "cancelled_branches": [],
            "late_arrivals": [],
            "not_propagated": [],
            "missing_targets": [],
            "risks": [],
        }

    cancelled: List[str] = []
    late_arrivals: List[Dict[str, Any]] = []
    not_propagated: List[str] = []
    risks: List[Dict[str, Any]] = []

    for branch in branches:
        if not branch.reachable:
            not_propagated.append(branch.branch_id)
            continue

        if branch.completed:
            # Signal is still delivered/acknowledged, but it is too late to
            # stop committed work.
            branch.cancel_received = True
            effects = _effect_dicts(branch)
            late_arrivals.append(
                {
                    "branch_id": branch.branch_id,
                    "state_before_cancel": branch.state,
                    "reason": "branch already completed before cancellation arrived",
                    "committed_effects": effects,
                }
            )
            for effect in effects:
                protected = effect["idempotency_protected"]
                risks.append(
                    {
                        "code": "LATE_CANCELLATION_COMMITTED_EFFECT",
                        "severity": "low" if protected else "high",
                        "branch_id": branch.branch_id,
                        "side_effect": effect["name"],
                        "idempotency_protected": protected,
                        "detail": (
                            "cancellation arrived after %s committed" % effect["name"]
                        ),
                        "required_action": (
                            "dedup via idempotency key"
                            if protected
                            else "compensate or reconcile before any retry"
                        ),
                    }
                )
            continue

        branch.state = "cancelled"
        branch.cancel_received = True
        cancelled.append(branch.branch_id)

    reached_set = set(explicit_reached) if explicit_reached is not None else None
    missing_targets: List[str] = []
    if reached_set is not None:
        missing_targets = [target for target in all_targets if target not in reached_set]
        for target in missing_targets:
            if target not in not_propagated:
                not_propagated.append(target)

    propagated_to_all = not not_propagated and not missing_targets

    return {
        "requested": True,
        "propagated_to_all": propagated_to_all,
        "targets": all_targets,
        "cancelled_branches": cancelled,
        "late_arrivals": late_arrivals,
        "not_propagated": sorted(set(not_propagated)),
        "missing_targets": missing_targets,
        "risks": risks,
    }
