"""Command line entry point.

Usage::

    python -m retryctx scenario.json
    cat scenario.json | python -m retryctx
    python -m retryctx --demo

The CLI is strictly local: it reads a JSON scenario and prints a JSON/text
report.  It never connects to an actual proxy or service mesh.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional

from .validator import validate_scenario


DEMO_SCENARIO: Dict[str, Any] = {
    "idempotency_key": "idem-demo-0001",
    "retry_count": 2,
    "original_request": {
        "method": "POST",
        "path": "/orders",
        "body_text": '{"order_id":"O-42","amount":9900}',
        "compress": "gzip",
    },
    "body_digest": None,  # filled dynamically below to stay correct
    "call_chain": [
        {
            "name": "client->gateway",
            "caller": "client",
            "callee": "gateway",
            "attempts": [
                {
                    "attempt_id": "gw-1",
                    "response_status": 503,
                    "retry_count": 0,
                    "idempotency_key": "idem-demo-0001",
                }
            ],
        },
        {
            "name": "gateway->order-service",
            "caller": "gateway",
            "callee": "order-service",
            "attempts": [
                {
                    "attempt_id": "ord-1",
                    "response_status": 504,
                    "retry_count": 0,
                    "idempotency_key": "idem-demo-0001",
                }
            ],
        },
        {
            "name": "order-service->payment-service",
            "caller": "order-service",
            "callee": "payment-service",
            "attempts": [
                {
                    "attempt_id": "pay-1",
                    "response_status": None,
                    "error": "read timeout",
                    "retry_count": 0,
                    "idempotency_key": "idem-demo-0001",
                    "side_effects": [
                        {
                            "name": "charge-card",
                            "idempotency_protected": False,
                        }
                    ],
                }
            ],
        },
    ],
}


def _demo_scenario() -> Dict[str, Any]:
    from .digest import canonical_body_digest
    from .model import HttpRequest

    request = HttpRequest.from_dict(DEMO_SCENARIO["original_request"])
    scenario = json.loads(json.dumps(DEMO_SCENARIO))
    scenario["body_digest"] = canonical_body_digest(
        request.body, request.content_encoding
    )
    for position in scenario["call_chain"]:
        for attempt in position["attempts"]:
            attempt["body_digest"] = scenario["body_digest"]
    return scenario


def render_text(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("idempotency_key : %s (%s)" % (
        report["idempotency_key"], report["idempotency_key_source"]
    ))
    digest = report["digest"]
    lines.append(
        "body digest     : %s (decompression changed wire digest: %s)"
        % (report["canonical_body_digest"],
           digest["digest_changed_after_decompression"])
    )
    lines.append("")
    lines.append("per-hop decisions:")
    for position in report["positions"]:
        lines.append(
            "  - %-38s %-22s direct=%s used=%s/%s"
            % (
                position["position"],
                position["decision"],
                position["direct_replay_allowed"],
                position["retries_used"],
                position["retry_budget"],
            )
        )
        for code in position["reason_codes"]:
            lines.append("      reason: %s" % code)

    lines.append("")
    lines.append("retryable positions:")
    if not report["retryable_positions"]:
        lines.append("  (none)")
    for item in report["retryable_positions"]:
        lines.append("  - %s [%s]" % (item["position"], item["mode"]))

    lines.append("")
    lines.append("duplicate side-effect risks:")
    if not report["duplicate_side_effect_risks"]:
        lines.append("  (none)")
    for risk in report["duplicate_side_effect_risks"]:
        if risk.get("position"):
            location = "%s/%s" % (risk["position"], risk["attempt_id"])
        else:
            location = "branch:%s" % risk.get("branch_id")
        lines.append(
            "  - [%s] %s @ %s protected=%s"
            % (
                risk["risk_level"].upper(),
                risk["side_effect"],
                location,
                risk["idempotency_protected"],
            )
        )
        lines.append("      action: %s" % risk["required_action"])

    cancellation = report.get("cancellation") or {}
    if cancellation.get("requested"):
        lines.append("")
        lines.append(
            "cancellation: propagated_to_all=%s late=%s"
            % (
                cancellation.get("propagated_to_all"),
                [item["branch_id"] for item in cancellation.get("late_arrivals", [])],
            )
        )
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m retryctx",
        description=(
            "Offline retry-context validator. Does NOT connect to any real "
            "proxy or service mesh."
        ),
    )
    parser.add_argument(
        "scenario",
        nargs="?",
        help="path to a JSON scenario file; reads stdin when omitted",
    )
    parser.add_argument(
        "--demo", action="store_true", help="validate the built-in demo scenario"
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="output format (default: json)",
    )
    args = parser.parse_args(argv)

    if args.demo:
        scenario = _demo_scenario()
    elif args.scenario:
        with open(args.scenario, "r", encoding="utf-8") as handle:
            scenario = json.load(handle)
    else:
        raw = sys.stdin.read()
        if not raw.strip():
            parser.error("no scenario provided on stdin (or use --demo)")
            return 2
        scenario = json.loads(raw)

    report = validate_scenario(scenario)
    if args.format == "text":
        print(render_text(report))
    else:
        json.dump(report, sys.stdout, ensure_ascii=False, indent=2, default=str)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
