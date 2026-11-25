import unittest

from retryctx import evaluate_retry, validate_scenario
from retryctx.model import HttpRequest


BODY = '{"order_id":"O-7","amount":4200}'
KEY = "idem-chain-7"


def canonical_digest():
    return HttpRequest.from_text(BODY).body_digest()


def chain_scenario(protected_charge=False, max_retries=2):
    digest = canonical_digest()
    charge_effect = {
        "name": "charge-card",
        "idempotency_protected": protected_charge,
    }
    return {
        "idempotency_key": KEY,
        "retry_count": max_retries,
        "body_digest": digest,
        "original_request": {
            "method": "POST",
            "path": "/checkout",
            "body_text": BODY,
        },
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
                        "idempotency_key": KEY,
                        "body_digest": digest,
                    }
                ],
            },
            {
                "name": "gateway->order",
                "caller": "gateway",
                "callee": "order-service",
                "attempts": [
                    {
                        "attempt_id": "ord-1",
                        "response_status": 504,
                        "retry_count": 0,
                        "idempotency_key": KEY,
                        "body_digest": digest,
                    }
                ],
            },
            {
                "name": "order->payment",
                "caller": "order-service",
                "callee": "payment-service",
                "attempts": [
                    {
                        "attempt_id": "pay-1",
                        "response_status": None,
                        "error": "read timeout",
                        "retry_count": 0,
                        "idempotency_key": KEY,
                        "body_digest": digest,
                        "side_effects": [charge_effect],
                    }
                ],
            },
        ],
    }


class RetryChainTests(unittest.TestCase):
    def test_same_logical_request_shares_one_key(self):
        report = validate_scenario(chain_scenario())
        self.assertEqual(report["idempotency_key"], KEY)
        # Every recorded attempt carried the same key (no mismatch findings).
        for finding in report["findings"]:
            self.assertNotEqual(finding["code"], "IDEMPOTENCY_KEY_MISMATCH")
            self.assertNotEqual(finding["code"], "IDEMPOTENCY_KEY_MISSING")
        self.assertEqual(len(report["positions"]), 3)

    def test_unprotected_committed_effect_blocks_every_upstream_retry(self):
        report = validate_scenario(chain_scenario(protected_charge=False))
        decisions = {p["position"]: p["decision"] for p in report["positions"]}
        self.assertEqual(decisions["order->payment"], "blocked")
        self.assertEqual(decisions["gateway->order"], "blocked")
        self.assertEqual(decisions["client->gateway"], "blocked")

        codes = set()
        for position in report["positions"]:
            codes.update(position["reason_codes"])
        self.assertIn("UNPROTECTED_SIDE_EFFECT_NO_DIRECT_REPLAY", codes)
        self.assertIn("DOWNSTREAM_UNPROTECTED_EFFECT_WOULD_REPLAY", codes)

        self.assertEqual(report["retryable_positions"], [])

        risks = report["duplicate_side_effect_risks"]
        self.assertEqual(len(risks), 1)
        risk = risks[0]
        self.assertEqual(risk["risk_level"], "high")
        self.assertFalse(risk["idempotency_protected"])
        self.assertEqual(risk["idempotency_key"], KEY)
        # Retrying any of the three positions re-triggers the charge.
        self.assertEqual(
            risk["would_retrigger_positions"],
            ["client->gateway", "gateway->order", "order->payment"],
        )
        self.assertIn("reconcile", risk["required_action"])

    def test_protected_effect_allows_only_guarded_idempotent_retry(self):
        report = validate_scenario(chain_scenario(protected_charge=True))
        for position in report["positions"]:
            self.assertEqual(position["decision"], "idempotent_retry_only")
            self.assertFalse(position["direct_replay_allowed"])
            self.assertTrue(position["retry_allowed"])

        modes = {
            item["position"]: item["mode"]
            for item in report["retryable_positions"]
        }
        self.assertEqual(
            modes,
            {
                "client->gateway": "idempotent_dedup",
                "gateway->order": "idempotent_dedup",
                "order->payment": "idempotent_dedup",
            },
        )
        risk = report["duplicate_side_effect_risks"][0]
        self.assertEqual(risk["risk_level"], "low")
        self.assertTrue(risk["idempotency_protected"])

    def test_safe_hop_without_effects_is_directly_retryable(self):
        digest = canonical_digest()
        report = evaluate_retry(
            original_request=HttpRequest.from_text(BODY),
            request_body_digest=digest,
            idempotency_key=KEY,
            retry_count=2,
            attempt_id="q-1",
            response_status=503,
        )
        self.assertEqual(len(report["positions"]), 1)
        position = report["positions"][0]
        self.assertEqual(position["decision"], "retryable")
        self.assertTrue(position["direct_replay_allowed"])
        self.assertEqual(position["suggested_attempt_id"], "q-1-r1")
        self.assertEqual(len(report["retryable_positions"]), 1)
        self.assertEqual(report["retryable_positions"][0]["mode"], "direct")
        evaluated = report["evaluated_attempt"]
        self.assertEqual(evaluated["attempt_id"], "q-1")
        self.assertTrue(evaluated["direct_replay_allowed"])

    def test_retry_budget_exhausted_blocks_position(self):
        digest = canonical_digest()
        scenario = {
            "idempotency_key": KEY,
            "retry_count": 1,
            "body_digest": digest,
            "call_chain": [
                {
                    "name": "gateway->order",
                    "attempts": [
                        {
                            "attempt_id": "a1",
                            "response_status": 503,
                            "retry_count": 0,
                            "idempotency_key": KEY,
                            "body_digest": digest,
                        },
                        {
                            "attempt_id": "a2",
                            "response_status": 503,
                            "retry_count": 1,
                            "idempotency_key": KEY,
                            "body_digest": digest,
                        },
                    ],
                }
            ],
        }
        report = validate_scenario(scenario)
        position = report["positions"][0]
        self.assertEqual(position["decision"], "blocked")
        self.assertIn("RETRY_BUDGET_EXHAUSTED", position["blockers"])
        self.assertEqual(position["retries_used"], 1)
        self.assertEqual(position["retry_budget"], 1)

    def test_downstream_key_mismatch_blocks_position(self):
        digest = canonical_digest()
        scenario = {
            "idempotency_key": KEY,
            "body_digest": digest,
            "call_chain": [
                {
                    "name": "gateway->order",
                    "attempts": [
                        {
                            "attempt_id": "a1",
                            "response_status": 502,
                            "idempotency_key": "idem-DIFFERENT",
                            "body_digest": digest,
                        }
                    ],
                }
            ],
        }
        report = validate_scenario(scenario)
        self.assertEqual(report["positions"][0]["decision"], "blocked")
        self.assertIn("KEY_MISMATCH", report["positions"][0]["blockers"])
        finding_codes = [f["code"] for f in report["findings"]]
        self.assertIn("IDEMPOTENCY_KEY_MISMATCH", finding_codes)

    def test_missing_propagated_key_blocks_position(self):
        digest = canonical_digest()
        scenario = {
            "idempotency_key": KEY,
            "body_digest": digest,
            "call_chain": [
                {
                    "name": "gateway->order",
                    "attempts": [
                        {
                            "attempt_id": "a1",
                            "response_status": 502,
                            "body_digest": digest,
                        }
                    ],
                }
            ],
        }
        report = validate_scenario(scenario)
        self.assertIn("KEY_MISSING", report["positions"][0]["blockers"])

    def test_success_and_client_error_are_not_retryable(self):
        digest = canonical_digest()
        ok = evaluate_retry(
            original_request=HttpRequest.from_text(BODY),
            request_body_digest=digest,
            idempotency_key=KEY,
            attempt_id="a1",
            response_status=200,
        )
        self.assertEqual(ok["positions"][0]["decision"], "not_required")
        self.assertEqual(ok["retryable_positions"], [])

        bad = evaluate_retry(
            original_request=HttpRequest.from_text(BODY),
            request_body_digest=digest,
            idempotency_key=KEY,
            attempt_id="a2",
            response_status=400,
        )
        self.assertEqual(bad["positions"][0]["decision"], "blocked")
        self.assertIn("STATUS_NOT_RETRYABLE", bad["positions"][0]["blockers"])

    def test_entry_generates_key_when_absent(self):
        report = evaluate_retry(
            original_request=HttpRequest.from_text(BODY),
            response_status=503,
            attempt_id="gw-1",
        )
        self.assertTrue(report["idempotency_key"].startswith("idem-"))
        self.assertEqual(report["idempotency_key_source"], "generated_at_entry")
        codes = [f["code"] for f in report["findings"]]
        self.assertIn("IDEMPOTENCY_KEY_GENERATED_AT_ENTRY", codes)

    def test_flat_hop_records_and_chain_alias_are_accepted(self):
        digest = canonical_digest()
        scenario = {
            "idempotency_key": KEY,
            "body_digest": digest,
            "chain": [
                {
                    "name": "gateway->order",
                    "attempt_id": "flat-1",
                    "response_status": 503,
                    "idempotency_key": KEY,
                    "body_digest": digest,
                }
            ],
        }
        report = validate_scenario(scenario)
        self.assertEqual(len(report["positions"]), 1)
        position = report["positions"][0]
        self.assertEqual(position["last_attempt_id"], "flat-1")
        self.assertEqual(position["decision"], "retryable")


if __name__ == "__main__":
    unittest.main()
