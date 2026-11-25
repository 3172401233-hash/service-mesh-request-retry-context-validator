import unittest

from retryctx import validate_scenario
from retryctx.cancellation import propagate_cancellation
from retryctx.model import Branch


DIGEST = "sha256:" + "a" * 64
KEY = "idem-cancel-1"


def retryable_chain():
    return [
        {
            "name": "gateway->order",
            "attempts": [
                {
                    "attempt_id": "ord-1",
                    "response_status": 503,
                    "idempotency_key": KEY,
                    "body_digest": DIGEST,
                }
            ],
        }
    ]


class CancellationPropagationTests(unittest.TestCase):
    def test_late_cancellation_still_reaches_every_branch(self):
        scenario = {
            "idempotency_key": KEY,
            "body_digest": DIGEST,
            "cancellation": {"requested": True, "arrival": "late"},
            "call_chain": retryable_chain(),
            "branches": [
                {
                    "branch_id": "payments",
                    "state": "completed",
                    "side_effects": [
                        {"name": "charge-card", "idempotency_protected": False}
                    ],
                },
                {"branch_id": "inventory", "state": "running"},
                {"branch_id": "loyalty", "state": "running"},
            ],
        }
        report = validate_scenario(scenario)
        cancellation = report["cancellation"]

        self.assertTrue(cancellation["requested"])
        self.assertTrue(cancellation["propagated_to_all"])
        self.assertEqual(
            sorted(cancellation["cancelled_branches"]),
            ["inventory", "loyalty"],
        )
        self.assertEqual(
            [item["branch_id"] for item in cancellation["late_arrivals"]],
            ["payments"],
        )

        finding_codes = [f["code"] for f in report["findings"]]
        self.assertIn("CANCELLATION_ARRIVED_LATE", finding_codes)
        self.assertIn("CANCELLATION_REQUESTED", finding_codes)

        # Cancellation blocks every retry position.
        self.assertEqual(report["retryable_positions"], [])
        for position in report["positions"]:
            self.assertEqual(position["decision"], "blocked")
            self.assertIn("CANCELLATION_REQUESTED", position["blockers"])

        # The committed payment effect is reported as a high duplicate risk.
        branch_risks = [
            risk
            for risk in report["duplicate_side_effect_risks"]
            if risk.get("branch_id") == "payments"
        ]
        self.assertEqual(len(branch_risks), 1)
        self.assertEqual(branch_risks[0]["risk_level"], "high")
        self.assertEqual(branch_risks[0]["idempotency_key"], KEY)

    def test_ontime_cancellation_cancels_running_branches_without_late_risk(self):
        branches = [Branch("inventory"), Branch("shipping")]
        report = propagate_cancellation(branches, requested=True)
        self.assertTrue(report["propagated_to_all"])
        self.assertEqual(
            sorted(report["cancelled_branches"]), ["inventory", "shipping"]
        )
        self.assertEqual(report["late_arrivals"], [])
        self.assertEqual(report["risks"], [])
        for branch in branches:
            self.assertEqual(branch.state, "cancelled")
            self.assertTrue(branch.cancel_received)

    def test_explicit_reached_list_missing_branch_is_detected(self):
        scenario = {
            "idempotency_key": KEY,
            "cancellation": {
                "requested": True,
                # Propagation evidence claims only inventory was reached.
                "reached": ["inventory"],
            },
            "branches": [
                {"branch_id": "inventory", "state": "running"},
                {"branch_id": "payments", "state": "running"},
            ],
            "call_chain": [],
        }
        report = validate_scenario(scenario)
        cancellation = report["cancellation"]
        self.assertFalse(cancellation["propagated_to_all"])
        self.assertEqual(cancellation["missing_targets"], ["payments"])
        codes = [f["code"] for f in report["findings"]]
        self.assertIn("CANCELLATION_NOT_PROPAGATED_TO_ALL_BRANCHES", codes)

    def test_unreachable_branch_breaks_full_propagation(self):
        branches = [
            Branch(branch_id="inventory"),
            Branch(branch_id="ghost", reachable=False),
        ]
        report = propagate_cancellation(branches, requested=True)
        self.assertFalse(report["propagated_to_all"])
        self.assertEqual(report["not_propagated"], ["ghost"])
        self.assertEqual(report["cancelled_branches"], ["inventory"])

    def test_late_cancellation_with_protected_effect_is_low_risk(self):
        branches = [
            Branch.from_dict(
                {
                    "branch_id": "payments",
                    "state": "completed",
                    "side_effects": [
                        {"name": "charge", "idempotency_protected": True}
                    ],
                }
            )
        ]
        report = propagate_cancellation(branches, requested=True)
        self.assertEqual(len(report["late_arrivals"]), 1)
        self.assertEqual(report["risks"][0]["severity"], "low")


if __name__ == "__main__":
    unittest.main()
