from helpers import T2, shipment
import unittest
import tempfile
import json
from pathlib import Path
from chainledger.types import ChainError
from chainledger.views.packets import verify_packet
from helpers import app, built, envelope, receive, transform, T1


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.app = app()
        self.addCleanup(self.app.close)
        self.app.consumer.submit(envelope())
        self.spec = {
            "id": "spec",
            "material": "alloy",
            "minimum_samples": 2,
            "maximum_defects": 0,
            "criteria": [
                {"name": "hardness", "unit": "HB", "lower": "10", "upper": "20"}
            ],
        }
        self.app.quality.specification(self.spec)

    def sample(self, ident, value, revision):
        return self.app.quality.sample(
            "inspection", ident, {"hardness": {"value": value, "unit": "HB"}}, revision
        )

    def test_inspection_holds_until_qualified_release(self):
        opened = self.app.quality.open("inspection", "raw", "spec")
        self.assertEqual(opened["state"], "sampling")
        self.assertEqual(opened["revision"], 1)
        self.assertTrue(self.app.graph.blocked("raw"))
        self.sample("sample1", "15", 1)
        self.assertFalse(self.app.quality.evaluate("inspection")["passed"])
        with self.assertRaises(ChainError):
            self.app.quality.disposition(
                "release", "inspection", 2, "reviewer", True, "Check"
            )
        self.sample("sample2", "16", 2)
        assessment = self.app.quality.evaluate("inspection")
        self.assertTrue(assessment["passed"])
        self.assertEqual(assessment["observed"], 2)
        released = self.app.quality.disposition(
            "release", "inspection", 3, "reviewer", True, "Accepted"
        )
        self.assertTrue(released["release"])
        self.assertEqual(self.app.graph.blocked("raw"), [])
        self.assertEqual(self.app.quality.get("inspection")["state"], "released")
        self.assertTrue(self.app.projections.verify_rebuild()["matching"])
        self.assertEqual(self.app.graph.blocked("raw"), [])

    def test_failed_measurements_retain_hold(self):
        self.app.quality.open("inspection", "raw", "spec")
        self.sample("sample1", "9", 1)
        self.sample("sample2", "16", 2)
        assessed = self.app.quality.evaluate("inspection")
        self.assertFalse(assessed["passed"])
        self.assertEqual(assessed["defects"], ["sample1"])
        with self.assertRaises(ChainError):
            self.app.quality.disposition(
                "release", "inspection", 3, "reviewer", True, "Cannot pass"
            )
        rejected = self.app.quality.disposition(
            "reject", "inspection", 3, "reviewer", False, "Out of range"
        )
        self.assertFalse(rejected["release"])
        self.assertEqual(self.app.quality.get("inspection")["state"], "rejected")
        self.assertTrue(self.app.graph.blocked("raw"))
        summary = self.app.quality.supplier_summary("supplier")["materials"]["alloy"]
        self.assertEqual(summary["rejected"], 1)
        self.assertEqual(summary["defective_samples"], 1)
        self.assertEqual(summary["samples"], 2)

    def test_units_and_sample_coverage_are_strict(self):
        self.app.quality.open("inspection", "raw", "spec")
        with self.assertRaises(ChainError):
            self.app.quality.sample("inspection", "sample1", {}, 1)
        with self.assertRaises(ChainError):
            self.app.quality.sample(
                "inspection",
                "sample1",
                {"hardness": {"value": "15", "unit": "wrong"}},
                1,
            )
        self.assertEqual(self.app.quality.get("inspection")["samples"], [])
        self.assertEqual(self.app.quality.get("inspection")["revision"], 1)
        self.sample("sample1", "15", 1)
        with self.assertRaises(ChainError):
            self.sample("sample2", "15", 1)
        self.assertEqual(len(self.app.quality.get("inspection")["samples"]), 1)

    def test_immutable_spec_and_sample_identity(self):
        self.assertEqual(self.app.quality.specification(self.spec)["id"], "spec")
        with self.assertRaises(ChainError):
            self.app.quality.specification({**self.spec, "minimum_samples": 3})
        self.app.quality.open("inspection", "raw", "spec")
        first = self.sample("sample1", "15", 1)
        self.assertEqual(self.sample("sample1", "15", 2), first)
        with self.assertRaises(ChainError):
            self.sample("sample1", "16", 2)
        self.assertEqual(self.app.quality.get("inspection")["revision"], 2)
        with self.assertRaises(ChainError):
            self.app.quality.specification(
                {**self.spec, "id": "invalid", "criteria": self.spec["criteria"] * 2}
            )

    def test_correction_invalidates_sampling_baseline(self):
        self.app.quality.open("inspection", "raw", "spec")
        self.sample("sample1", "15", 1)
        self.sample("sample2", "15", 2)
        self.app.projections.correct(
            "correct",
            "e0",
            envelope(ops=[receive(quantity="12")]),
            "Quantity corrected",
        )
        assessment = self.app.quality.evaluate("inspection")
        self.assertFalse(assessment["baseline_unchanged"])
        self.assertFalse(assessment["passed"])
        self.assertTrue(self.app.graph.blocked("raw"))
        with self.assertRaises(ChainError):
            self.app.quality.disposition(
                "release", "inspection", 3, "reviewer", True, "Changed baseline"
            )

    def test_inspection_blocks_transform(self):
        self.app.quality.open("inspection", "raw", "spec")
        attempted = self.app.consumer.submit(
            envelope("transform", "f", 0, [transform()], T1)
        )
        self.assertEqual(attempted["state"], "rejected")
        self.assertEqual(attempted["reason"]["code"], "hold")
        self.assertEqual(self.app.graph.stock("raw"), 10)
        self.assertTrue(self.app.projections.verify_rebuild()["matching"])
        self.assertEqual(len(self.app.graph.blocked("raw")), 1)

    def test_repeated_disposition_is_idempotent(self):
        self.app.quality.open("inspection", "raw", "spec")
        self.sample("sample1", "15", 1)
        self.sample("sample2", "15", 2)
        first = self.app.quality.disposition(
            "release", "inspection", 3, "reviewer", True, "Accepted"
        )
        second = self.app.quality.disposition(
            "release", "inspection", 3, "reviewer", True, "Accepted"
        )
        self.assertEqual(first, second)
        self.assertEqual(self.app.quality.get("inspection")["revision"], 4)
        with self.assertRaises(ChainError):
            self.app.quality.disposition(
                "release", "inspection", 3, "other", True, "Accepted"
            )
        self.assertEqual(self.app.graph.blocked("raw"), [])


class PacketTests(unittest.TestCase):
    def setUp(self):
        self.app = built(app())
        self.addCleanup(self.app.close)

    def test_packet_evidence_is_frozen(self):
        created = self.app.packets.create("packet", "sh")
        self.assertFalse(created["reused"])
        before = self.app.packets.get("packet")
        self.assertEqual(before["payload"]["shipment"]["quantity"], "4")
        self.assertEqual(len(before["payload"]["graph"]["nodes"]), 2)
        self.assertEqual(len(before["payload"]["transformations"]), 1)
        self.assertTrue(self.app.packets.create("packet", "sh")["reused"])
        self.app.recalls.open("r", "raw", "Review")
        self.assertEqual(self.app.packets.get("packet"), before)
        with self.assertRaises(ChainError):
            self.app.packets.create("packet", "sh")
        second = self.app.packets.create("later", "sh")
        self.assertNotEqual(created["digest"], second["digest"])
        self.assertTrue(self.app.packets.get("later")["payload"]["current_blocks"])

    def test_packet_export_verification_and_tampering(self):
        self.app.packets.create("packet", "sh")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "packet.json"
            self.app.packets.export("packet", path)
            result = verify_packet(path)
            self.assertEqual(result["id"], "packet")
            self.assertEqual(result["shipment"], "sh")
            self.assertEqual(result["lots"], 2)
            with self.assertRaises(ChainError):
                self.app.packets.export("packet", path)
            data = json.loads(path.read_text())
            data["payload"]["shipment"]["quantity"] = "99"
            path.write_text(json.dumps(data))
            with self.assertRaises(ChainError) as caught:
                verify_packet(path)
            self.assertEqual(caught.exception.code, "integrity")

    def test_missing_packet_or_shipment_is_explicit(self):
        with self.assertRaises(ChainError):
            self.app.packets.create("packet", "missing")
        with self.assertRaises(ChainError):
            self.app.packets.get("missing")
        self.assertEqual(self.app.journal.rows("SELECT * FROM shipment_packets"), [])
        self.assertEqual(self.app.graph.stock("child"), 5)


class PickingTests(unittest.TestCase):
    def setUp(self):
        self.app = built(app())
        self.addCleanup(self.app.close)
        self.requirements = [
            {"material": "alloy", "unit": "kg", "quantity": "3", "locations": ["B"]}
        ]

    def test_dispatch_creates_atomic_shipments(self):
        plan = self.app.picking.plan("pick", "customer2", self.requirements, T2)
        self.assertTrue(plan["ready"])
        self.assertFalse(plan["reservation"])
        self.assertEqual(plan["allocations"][0]["quantity"], "3")
        dispatched = self.app.picking.dispatch("pick", "dispatch", "f", 2)
        self.assertEqual(dispatched["state"], "dispatched")
        self.assertEqual(self.app.graph.stock("child"), 2)
        self.assertEqual(dispatched["result"]["shipments"], ["dispatch:0"])
        self.assertEqual(
            self.app.picking.dispatch("pick", "dispatch", "f", 2), dispatched
        )
        self.assertEqual(self.app.picking.queue(), [])
        self.assertTrue(self.app.trace.conservation_report()["conserved"])

    def test_inventory_change_invalidates_plan(self):
        self.app.picking.plan("pick", "customer2", self.requirements, T2)
        self.app.consumer.submit(
            envelope("ship-more", "f", 2, [shipment("more", "child", "1", "more")], T2)
        )
        with self.assertRaises(ChainError):
            self.app.picking.dispatch("pick", "dispatch", "f", 3)
        self.assertEqual(self.app.graph.stock("child"), 4)
        self.assertEqual(self.app.picking.get("pick")["state"], "planned")
        self.assertIsNone(
            self.app.journal.one("SELECT id FROM envelopes WHERE id='dispatch'")
        )

    def test_shortage_and_cancellation_are_visible(self):
        required = [{**self.requirements[0], "quantity": "6"}]
        plan = self.app.picking.plan("pick", "customer2", required, T2)
        self.assertFalse(plan["ready"])
        self.assertEqual(plan["shortages"][0]["quantity"], "1")
        with self.assertRaises(ChainError):
            self.app.picking.dispatch("pick", "dispatch", "f", 2)
        cancelled = self.app.picking.cancel("pick", "Reschedule")
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(self.app.graph.stock("child"), 5)
        self.assertEqual(self.app.picking.queue(), [])

    def test_new_quality_hold_blocks_dispatch(self):
        self.app.picking.plan("pick", "customer2", self.requirements, T2)
        self.app.recalls.open("r", "raw", "Review")
        with self.assertRaises(ChainError):
            self.app.picking.dispatch("pick", "dispatch", "f", 2)
        self.assertEqual(self.app.graph.stock("child"), 5)
        plan = self.app.picking.plan("new", "customer2", self.requirements, T2)
        self.assertFalse(plan["ready"])
        self.assertEqual(plan["allocations"], [])
