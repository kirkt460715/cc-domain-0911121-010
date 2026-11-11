import json
import tempfile
import unittest
from pathlib import Path
from decimal import Decimal
from chainledger.types import ChainError, amount, key, decode, timestamp
from chainledger.events.protocol import Envelope, operation, dependencies
from helpers import app, envelope, receive, transform, shipment, T0, T1


class ProtocolTests(unittest.TestCase):
    def test_exact_quantities(self):
        self.assertEqual(amount("0.000000001"), Decimal("0.000000001"))
        self.assertEqual(amount("9007199254740993"), Decimal("9007199254740993"))
        for value in [
            "NaN",
            "Infinity",
            "-1",
            "0",
            "0.0000000001",
            True,
            1.5,
            {},
            None,
        ]:
            with self.subTest(value=value):
                with self.assertRaises(ChainError):
                    amount(value)
        self.assertEqual(amount("0", allow_zero=True), 0)

    def test_json_duplicate_and_nonfinite(self):
        for value in ['{"a":1,"a":2}', '{"a":NaN}', "invalid"]:
            with self.assertRaises(ChainError):
                decode(value)
        self.assertEqual(decode('{"a":[1,2]}'), {"a": [1, 2]})
        self.assertEqual(timestamp("2026-11-11T08:00:00+08:00"), timestamp(T0))
        with self.assertRaises(ChainError):
            timestamp("2026-11-11T00:00:00")
        with self.assertRaises(ChainError):
            key("../outside")

    def test_envelope_identity_and_duplicate_operations(self):
        data = envelope()
        parsed = Envelope(data)
        self.assertEqual(parsed.source, "s")
        self.assertEqual(parsed.offset, 0)
        self.assertEqual(Envelope(parsed.as_dict()).digest, parsed.digest)
        with self.assertRaises(ChainError):
            Envelope(envelope(ops=[receive(), receive()]))
        with self.assertRaises(ChainError):
            Envelope({**data, "offset": True})
        with self.assertRaises(ChainError):
            Envelope({**data, "extra": "no"})
        with self.assertRaises(ChainError):
            Envelope({**data, "operations": []})

    def test_transform_validation_and_dependencies(self):
        operation(transform())
        with self.assertRaises(ChainError):
            operation(
                transform(outputs=[{"lot": "raw", "quantity": "9", "location": "B"}])
            )
        with self.assertRaises(ChainError):
            operation(
                transform(
                    inputs=[
                        {"lot": "raw", "quantity": "5"},
                        {"lot": "raw", "quantity": "5"},
                    ]
                )
            )
        self.assertEqual(dependencies(Envelope(envelope(ops=[transform()]))), ["raw"])
        self.assertEqual(
            dependencies(Envelope(envelope(ops=[receive(), transform()]))), []
        )
        with self.assertRaises(ChainError):
            operation({"id": "x", "kind": "unknown"})


class ConsumerTests(unittest.TestCase):
    def setUp(self):
        self.app = app()
        self.addCleanup(self.app.close)

    def test_source_offset_gap_and_drain(self):
        later = envelope("later", "s", 1, [receive("next", ident="r-next")])
        self.assertEqual(self.app.consumer.submit(later)["state"], "pending")
        self.assertEqual(self.app.consumer.gaps("s")["gaps"], [{"first": 0, "last": 0}])
        first = self.app.consumer.submit(envelope())
        self.assertEqual(first["state"], "applied")
        self.assertEqual(self.app.consumer.status("later")["state"], "applied")
        self.assertEqual(self.app.consumer.gaps("s")["next_offset"], 2)
        self.assertEqual(self.app.consumer.pending(), [])
        self.assertTrue(self.app.check()["conservation"]["conserved"])

    def test_cross_source_dependency_is_retried(self):
        child = envelope("child", "f", 0, [transform()], T1)
        first = self.app.consumer.submit(child)
        self.assertEqual(first["state"], "blocked")
        self.assertEqual(first["reason"]["code"], "dependency")
        self.app.consumer.submit(envelope())
        final = self.app.consumer.status("child")
        self.assertEqual(final["state"], "applied")
        self.assertEqual(self.app.graph.stock("raw"), 0)
        self.assertEqual(self.app.graph.stock("child"), 9)
        self.assertEqual(self.app.trace.topology()["maximum_depth"], 1)

    def test_idempotency_and_offset_conflicts(self):
        self.app.consumer.submit(envelope())
        duplicate = self.app.consumer.submit(envelope())
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(self.app.graph.stock("raw"), 10)
        with self.assertRaises(ChainError) as caught:
            self.app.consumer.submit(envelope(ops=[receive(quantity="11")]))
        self.assertEqual(caught.exception.code, "conflict")
        with self.assertRaises(ChainError):
            self.app.consumer.submit(envelope(ident="different"))
        self.assertEqual(self.app.consumer.gaps("s")["next_offset"], 1)

    def test_invalid_transaction_rolls_back_all_operations(self):
        events = [receive(), transform(waste="2")]
        result = self.app.consumer.submit(envelope(ops=events))
        self.assertEqual(result["state"], "rejected")
        self.assertEqual(result["reason"]["code"], "conservation")
        self.assertEqual(self.app.inventory.list()["items"], [])
        self.assertEqual(
            self.app.journal.one("SELECT COUNT(*) AS n FROM operations")["n"], 0
        )
        self.assertEqual(self.app.consumer.gaps("s")["next_offset"], 0)
        skipped = self.app.consumer.skip(
            "e0", "Source operator confirms malformed transaction"
        )
        self.assertEqual(self.app.consumer.status("e0")["state"], "skipped")
        self.assertEqual(self.app.consumer.gaps("s")["next_offset"], 1)
        self.assertEqual(skipped["pending"], [])

    def test_rejected_head_stops_later_events(self):
        self.app.consumer.submit(envelope(ops=[receive(), transform(waste="2")]))
        self.app.consumer.submit(
            envelope("later", "s", 1, [receive("valid", ident="valid")])
        )
        self.assertEqual(self.app.consumer.status("later")["state"], "pending")
        result = self.app.consumer.skip("e0", "Invalid conservation equation")
        self.assertEqual(result["applied"], ["later"])
        self.assertEqual(self.app.graph.stock("valid"), 10)
        with self.assertRaises(ChainError):
            self.app.consumer.skip("later", "Cannot skip applied event")

    def test_missing_source_and_stale_offsets(self):
        with self.assertRaises(ChainError):
            self.app.consumer.submit(envelope(source="unknown"))
        self.app.consumer.register("seeded", 5)
        with self.assertRaises(ChainError):
            self.app.consumer.submit(envelope(source="seeded", offset=4))
        self.assertEqual(self.app.consumer.register("seeded")["next_offset"], 5)
        with self.assertRaises(ChainError):
            self.app.consumer.status("missing")
        with self.assertRaises(ChainError):
            self.app.consumer.gaps("missing")

    def test_exchange_import_prevalidation_is_atomic(self):
        good = json.dumps(envelope())
        with self.assertRaises(ChainError):
            self.app.exchange.import_jsonl(good + "\n{broken}")
        self.assertEqual(self.app.inventory.list()["items"], [])
        result = self.app.exchange.import_jsonl(good)
        self.assertEqual(result["received"], 1)
        self.assertEqual(result["drain"]["applied"], ["e0"])
        retry = self.app.exchange.import_jsonl(good)
        self.assertEqual(retry["duplicates"], 1)
        with self.assertRaises(ChainError):
            self.app.exchange.import_jsonl(good + "\n" + good)

    def test_exchange_export_and_manifest_comparison(self):
        self.app.consumer.submit(envelope())
        exported = self.app.exchange.export_source("s")
        self.assertEqual(exported["count"], 1)
        self.assertEqual(Envelope(decode(exported["content"])).id, "e0")
        digest = Envelope(envelope()).digest
        matching = self.app.exchange.compare_source(
            "s", {"source": "s", "events": [{"offset": 0, "digest": digest}]}
        )
        self.assertEqual(matching["conflicts"], [])
        self.assertEqual(matching["local_only"], [])
        mismatched = self.app.exchange.compare_source(
            "s",
            {
                "source": "s",
                "events": [
                    {"offset": 0, "digest": "a" * 64},
                    {"offset": 2, "digest": "b" * 64},
                ],
            },
        )
        self.assertEqual(mismatched["conflicts"], [0])
        self.assertEqual(mismatched["remote_only"], [2])
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "events.jsonl"
            self.app.exchange.write_source("s", target)
            self.assertEqual(target.read_text(), exported["content"])
            with self.assertRaises(ChainError):
                self.app.exchange.write_source("s", target)
        self.assertIn("s,1,1,1,0,0,0", self.app.exchange.offsets_csv())


class MaterialOperationTests(unittest.TestCase):
    def setUp(self):
        self.app = app()
        self.addCleanup(self.app.close)
        self.app.consumer.submit(envelope())

    def submit(self, ops, offset=0, ident="op"):
        return self.app.consumer.submit(envelope(ident, "f", offset, ops, T1))

    def test_split_stock_and_global_conservation(self):
        result = self.submit(
            [
                transform(
                    outputs=[
                        {"lot": "a", "quantity": "4", "location": "B"},
                        {"lot": "b", "quantity": "5", "location": "C"},
                    ]
                )
            ]
        )
        self.assertEqual(result["state"], "applied")
        self.assertEqual(self.app.graph.stock("a"), 4)
        self.assertEqual(self.app.graph.stock("b"), 5)
        self.assertEqual(self.app.graph.stock("raw"), 0)
        report = self.app.trace.conservation_report()["materials"][0]
        self.assertEqual(report["received"], "10")
        self.assertEqual(report["stock"], "9")
        self.assertEqual(report["waste"], "1")
        self.assertEqual(report["difference"], "0")

    def test_merge_requires_same_material_and_unit(self):
        self.app.consumer.submit(
            envelope(
                "other", "s", 1, [receive("other", "5", "other", material="copper")]
            )
        )
        op = transform(
            inputs=[{"lot": "raw", "quantity": "5"}, {"lot": "other", "quantity": "5"}]
        )
        result = self.submit([op])
        self.assertEqual(result["state"], "rejected")
        self.assertEqual(result["reason"]["code"], "material")
        self.assertEqual(self.app.graph.stock("raw"), 10)
        self.assertEqual(self.app.graph.stock("other"), 5)

    def test_transfer_preserves_quantity(self):
        move = {
            "id": "move",
            "kind": "move",
            "lot": "raw",
            "from": "A",
            "to": "B",
            "quantity": "3",
        }
        self.assertEqual(self.submit([move])["state"], "applied")
        self.assertEqual(self.app.graph.balance("raw", "A"), 7)
        self.assertEqual(self.app.graph.balance("raw", "B"), 3)
        self.assertEqual(self.app.graph.stock("raw"), 10)
        bad = {**move, "id": "move-too-much", "quantity": "8"}
        result = self.submit([bad], 1, "bad")
        self.assertEqual(result["reason"]["code"], "insufficient")
        self.assertEqual(self.app.graph.balance("raw", "A"), 7)
        self.assertTrue(self.app.graph.conservation()["conserved"])

    def test_partial_return_and_overreturn(self):
        ship = shipment(lot="raw", location="A")
        self.assertEqual(self.submit([ship])["state"], "applied")
        returned = {
            "id": "ret",
            "kind": "return",
            "shipment": "sh",
            "quantity": "2",
            "location": "C",
        }
        self.assertEqual(self.submit([returned], 1, "return")["state"], "applied")
        self.assertEqual(self.app.graph.stock("raw"), 8)
        self.assertEqual(self.app.inventory.shipments()[0]["outstanding"], "2")
        result = self.submit([{**returned, "id": "over", "quantity": "3"}], 2, "over")
        self.assertEqual(result["reason"]["code"], "conservation")
        self.assertEqual(self.app.graph.stock("raw"), 8)
        self.assertEqual(
            self.app.trace.conservation_report()["materials"][0]["shipped"], "2"
        )

    def test_ancestor_hold_blocks_descendant_shipment(self):
        self.submit([transform()])
        hold = {
            "id": "hold",
            "kind": "hold",
            "lot": "raw",
            "reason": "Synthetic quality concern",
        }
        self.submit([hold], 1, "hold-event")
        result = self.submit([shipment()], 2, "blocked-shipment")
        self.assertEqual(result["state"], "rejected")
        self.assertEqual(result["reason"]["code"], "hold")
        self.assertEqual(self.app.graph.stock("child"), 9)
        self.assertEqual(self.app.graph.blocked("child")[0]["lot"], "raw")
        self.app.consumer.skip("blocked-shipment", "Wait for release")
        release = {"id": "release", "kind": "release", "lot": "raw", "hold": "hold"}
        self.submit([release], 3, "release-event")
        self.assertEqual(self.app.graph.blocked("child"), [])
        self.assertEqual(self.submit([shipment()], 4, "shipment")["state"], "applied")

    def test_scrap_reduces_stock_without_shipping(self):
        scrap = {
            "id": "scrap",
            "kind": "scrap",
            "lot": "raw",
            "location": "A",
            "quantity": "2",
            "reason": "damaged",
        }
        self.assertEqual(self.submit([scrap])["state"], "applied")
        self.assertEqual(self.app.graph.stock("raw"), 8)
        totals = self.app.trace.conservation_report()["materials"][0]
        self.assertEqual(totals["scrap"], "2")
        self.assertEqual(totals["shipped"], "0")
        self.assertEqual(totals["difference"], "0")

    def test_existing_output_lot_cannot_create_cycle(self):
        self.app.consumer.submit(
            envelope("other", "s", 1, [receive("other", "10", "other")])
        )
        op = transform(outputs=[{"lot": "other", "quantity": "9", "location": "B"}])
        result = self.submit([op])
        self.assertEqual(result["reason"]["code"], "conflict")
        self.assertEqual(self.app.trace.topology()["maximum_depth"], 0)
        self.assertEqual(self.app.graph.stock("raw"), 10)
        self.assertEqual(self.app.journal.rows("SELECT * FROM edges"), [])

    def test_transform_cannot_predate_input(self):
        result = self.app.consumer.submit(
            envelope("past", "f", 0, [transform()], at="2026-11-10T00:00:00Z")
        )
        self.assertEqual(result["reason"]["code"], "time")
        self.assertEqual(self.app.graph.stock("raw"), 10)
        self.assertEqual(self.app.inventory.list()["items"][0]["id"], "raw")
