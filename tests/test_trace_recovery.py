import unittest
import tempfile
import threading
import json
import io
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr
from chainledger.types import ChainError
from chainledger.service import SupplyChain
from chainledger.commands import main, dispatch
from helpers import app, built, envelope, receive, transform, T0, T1, T2


class TraceTests(unittest.TestCase):
    def setUp(self):
        self.app = built(app())
        self.addCleanup(self.app.close)

    def test_paths_and_independent_lots(self):
        self.app.consumer.submit(
            envelope("unrelated", "s", 1, [receive("other", "3", "other")])
        )
        self.assertEqual(self.app.trace.path("raw", "child"), ["raw", "child"])
        self.assertEqual(self.app.trace.path("raw", "raw"), ["raw"])
        self.assertIsNone(self.app.trace.path("other", "child"))
        self.assertEqual(self.app.graph.ancestors("child"), ["raw"])
        self.assertEqual(self.app.graph.descendants("raw"), ["child"])
        self.assertEqual(self.app.graph.ancestors("raw"), [])
        subgraph = self.app.trace.subgraph("child", "upstream")
        self.assertEqual({item["id"] for item in subgraph["nodes"]}, {"raw", "child"})
        self.assertEqual(len(subgraph["edges"]), 1)
        with self.assertRaises(ChainError):
            self.app.trace.subgraph("raw", maximum=1)

    def test_root_contributions_of_merge(self):
        self.app.consumer.submit(
            envelope("second", "s", 1, [receive("second", "9", "second")])
        )
        op = transform(
            "merge",
            inputs=[
                {"lot": "child", "quantity": "5"},
                {"lot": "second", "quantity": "5"},
            ],
            outputs=[{"lot": "merged", "quantity": "10", "location": "C"}],
            waste="0",
        )
        self.app.consumer.submit(envelope("merged", "f", 2, [op], T2))
        result = self.app.trace.root_contributions("merged")
        contributions = {item["root"]: item for item in result["contributions"]}
        self.assertEqual(set(contributions), {"raw", "second"})
        self.assertEqual(contributions["raw"]["fraction"], "0.5")
        self.assertEqual(contributions["second"]["attributed_quantity"], "5")
        self.assertIn("homogeneous", result["assumption"])
        self.assertEqual(self.app.trace.topology()["maximum_depth"], 2)
        self.assertTrue(self.app.trace.conservation_report()["conserved"])

    def test_impact_counts_only_outstanding_shipments(self):
        impact = self.app.trace.impact("raw")
        self.assertEqual(impact["lots"], ["raw", "child"])
        self.assertEqual(impact["shipments"][0]["outstanding"], "4")
        self.assertEqual(impact["customers"][0]["customer"], "customer")
        self.assertEqual(impact["stock"][0]["quantity"], "5")
        returned = {
            "id": "return",
            "kind": "return",
            "shipment": "sh",
            "quantity": "4",
            "location": "B",
        }
        self.app.consumer.submit(envelope("return", "f", 2, [returned], T2))
        after = self.app.trace.impact("raw")
        self.assertEqual(after["shipments"], [])
        self.assertEqual(after["customers"], [])
        self.assertEqual(after["stock"][0]["quantity"], "9")

    def test_inventory_filters_and_yield(self):
        all_lots = self.app.inventory.list(include_empty=True, limit=1)
        self.assertEqual(len(all_lots["items"]), 1)
        next_page = self.app.inventory.list(
            include_empty=True, limit=1, after=all_lots["next"]
        )
        self.assertEqual(len(next_page["items"]), 1)
        self.assertNotEqual(all_lots["items"][0]["id"], next_page["items"][0]["id"])
        self.assertIsNone(next_page["next"])
        self.assertEqual(self.app.inventory.list(location="A")["items"], [])
        self.assertEqual(self.app.inventory.list(material="none")["items"], [])
        yield_report = self.app.inventory.process_yields()[0]
        self.assertEqual(yield_report["input"], "10")
        self.assertEqual(yield_report["output"], "9")
        self.assertEqual(yield_report["yield"], "0.9")
        self.assertEqual(
            self.app.inventory.supplier_exposure("supplier")["affected_lots"],
            ["child", "raw"],
        )
        self.assertEqual(len(self.app.inventory.operations("child")), 2)


class RecallTests(unittest.TestCase):
    def setUp(self):
        self.app = built(app())
        self.addCleanup(self.app.close)

    def test_recall_acknowledgement_and_close(self):
        recall = self.app.recalls.open("r", "raw", "Synthetic quality review")
        self.assertEqual(recall["revision"], 1)
        self.assertEqual(recall["state"], "open")
        self.assertEqual(
            self.app.recalls.open("r", "raw", "Synthetic quality review"), recall
        )
        with self.assertRaises(ChainError):
            self.app.recalls.close("r", 1)
        acknowledged = self.app.recalls.acknowledge("r", "customer", {"sh": "4"}, 1)
        self.assertEqual(acknowledged["revision"], 2)
        closed = self.app.recalls.close("r", 2)
        self.assertEqual(closed["state"], "closed")
        self.assertEqual(closed["revision"], 3)
        self.assertTrue(self.app.graph.blocked("child"))
        with self.assertRaises(ChainError):
            self.app.recalls.acknowledge("r", "customer", {"sh": "4"}, 3)

    def test_partial_acknowledgement_prevents_close(self):
        self.app.recalls.open("r", "raw", "Review")
        self.app.recalls.acknowledge("r", "customer", {"sh": "2"}, 1)
        with self.assertRaises(ChainError) as caught:
            self.app.recalls.close("r", 2)
        self.assertEqual(caught.exception.code, "state")
        self.assertEqual(self.app.recalls.get("r")["state"], "open")
        with self.assertRaises(ChainError):
            self.app.recalls.acknowledge("r", "customer", {"sh": "5"}, 2)
        with self.assertRaises(ChainError):
            self.app.recalls.acknowledge("r", "other", {"sh": "4"}, 2)
        with self.assertRaises(ChainError):
            self.app.recalls.acknowledge("r", "customer", {"sh": "4"}, 1)

    def test_refresh_invalidates_acknowledgements(self):
        self.app.recalls.open("r", "raw", "Review")
        self.app.recalls.acknowledge("r", "customer", {"sh": "4"}, 1)
        returned = {
            "id": "return",
            "kind": "return",
            "shipment": "sh",
            "quantity": "1",
            "location": "B",
        }
        self.app.consumer.submit(envelope("return", "f", 2, [returned], T2))
        with self.assertRaises(ChainError) as caught:
            self.app.recalls.close("r", 2)
        self.assertEqual(caught.exception.code, "conflict")
        refreshed = self.app.recalls.refresh("r", 2)
        self.assertEqual(refreshed["revision"], 3)
        self.assertEqual(refreshed["acknowledgements"], [])
        self.assertEqual(refreshed["impact"]["shipments"][0]["outstanding"], "3")
        self.app.recalls.acknowledge("r", "customer", {"sh": "3"}, 3)
        self.assertEqual(self.app.recalls.close("r", 4)["state"], "closed")

    def test_rebuild_preserves_recall_hold(self):
        self.app.recalls.open("r", "raw", "Review")
        before = self.app.projections.manifest()
        self.assertTrue(self.app.projections.verify_rebuild()["matching"])
        self.assertEqual(self.app.projections.manifest(), before)
        self.assertEqual(self.app.graph.blocked("child")[0]["id"], "recall:r")
        self.assertEqual(self.app.recalls.get("r")["state"], "open")
        self.assertTrue(self.app.check()["conservation"]["conserved"])

    def test_certificate_validity_and_revocation(self):
        certificate = self.app.recalls.certificate(
            "cert", "alloy", T0, "2027-01-01T00:00:00Z"
        )
        self.assertFalse(certificate["revoked"])
        data = envelope(
            "certified",
            "s",
            1,
            [receive("certified", "2", "certified", certificate="cert")],
        )
        self.assertEqual(self.app.consumer.submit(data)["state"], "applied")
        revoked = self.app.recalls.revoke_certificate("cert", "Certificate invalidated")
        self.assertEqual(revoked["affected"], ["certified"])
        self.assertTrue(self.app.graph.blocked("certified"))
        self.assertTrue(self.app.projections.verify_rebuild()["matching"])
        self.assertFalse(
            self.app.recalls.revoke_certificate("cert", "Repeated")["changed"]
        )
        with self.assertRaises(ChainError):
            self.app.recalls.certificate("cert", "copper", T0, T1)

    def test_receipt_waits_for_missing_certificate(self):
        data = envelope(
            "certified",
            "s",
            1,
            [receive("certified", "2", "certified", certificate="missing-cert")],
        )
        result = self.app.consumer.submit(data)
        self.assertEqual(result["state"], "blocked")
        self.app.recalls.certificate(
            "missing-cert", "alloy", T0, "2027-01-01T00:00:00Z"
        )
        self.app.consumer.drain()
        self.assertEqual(self.app.consumer.status("certified")["state"], "applied")
        self.assertEqual(self.app.graph.stock("certified"), 2)


class ProjectionTests(unittest.TestCase):
    def setUp(self):
        self.app = built(app())
        self.addCleanup(self.app.close)

    def test_rebuild_and_checkpoint(self):
        check = self.app.projections.verify_rebuild()
        self.assertTrue(check["matching"])
        checkpoint = self.app.projections.checkpoint("cp")
        self.assertFalse(checkpoint["reused"])
        self.assertTrue(self.app.projections.checkpoint("cp")["reused"])
        self.assertTrue(
            self.app.projections.compare_checkpoint("cp")["same_projection"]
        )
        self.app.consumer.submit(envelope("new", "s", 1, [receive("new", "1", "new")]))
        changed = self.app.projections.compare_checkpoint("cp")
        self.assertFalse(changed["same_projection"])
        self.assertIn("lots", [item["table"] for item in changed["changes"]])
        with self.assertRaises(ChainError):
            self.app.projections.checkpoint("cp")

    def test_valid_correction_rebuilds_downstream(self):
        replacement = envelope(ops=[receive(quantity="12")])
        result = self.app.projections.correct(
            "c", "e0", replacement, "Correct source quantity"
        )
        self.assertFalse(result["reused"])
        self.assertNotEqual(result["before"], result["after"])
        self.assertEqual(self.app.graph.stock("raw"), 2)
        self.assertEqual(self.app.graph.stock("child"), 5)
        self.assertEqual(
            self.app.trace.conservation_report()["materials"][0]["received"], "12"
        )
        self.assertTrue(
            self.app.projections.correct(
                "c", "e0", replacement, "Correct source quantity"
            )["reused"]
        )
        self.assertTrue(self.app.projections.verify_rebuild()["matching"])
        self.assertEqual(self.app.consumer.status("e0")["state"], "applied")

    def test_invalid_correction_rolls_back_projection(self):
        before = self.app.projections.manifest()
        with self.assertRaises(ChainError) as caught:
            self.app.projections.correct(
                "bad",
                "e0",
                envelope(ops=[receive(quantity="5")]),
                "Impossible correction",
            )
        self.assertEqual(caught.exception.code, "insufficient")
        self.assertEqual(self.app.projections.manifest(), before)
        self.assertEqual(self.app.journal.rows("SELECT * FROM adjustments"), [])
        self.assertFalse(self.app.graph.replaying)
        self.assertTrue(self.app.check()["conservation"]["conserved"])

    def test_correction_requires_same_envelope_identity(self):
        with self.assertRaises(ChainError):
            self.app.projections.correct(
                "bad", "e0", envelope(ident="other"), "Wrong id"
            )
        with self.assertRaises(ChainError):
            self.app.projections.correct(
                "bad", "e0", envelope(offset=9), "Wrong offset"
            )
        with self.assertRaises(ChainError):
            self.app.projections.correct("bad", "absent", envelope(), "Missing target")
        self.assertEqual(self.app.graph.stock("child"), 5)
        self.assertEqual(self.app.journal.rows("SELECT * FROM adjustments"), [])

    def test_correction_preserves_external_recall(self):
        self.app.recalls.open("r", "raw", "Review")
        self.app.projections.correct(
            "c", "e0", envelope(ops=[receive(quantity="12")]), "Correct source"
        )
        self.assertTrue(self.app.graph.blocked("child"))
        self.assertEqual(self.app.recalls.get("r")["state"], "open")
        with self.assertRaises(ChainError):
            self.app.recalls.close("r", 1)
        refreshed = self.app.recalls.refresh("r", 1)
        self.assertEqual(refreshed["revision"], 2)
        self.assertEqual(len(refreshed["impact"]["stock"]), 2)

    def test_tampered_projection_fails_rebuild_comparison(self):
        with self.app.journal.transaction() as con:
            con.execute("UPDATE balances SET quantity='6' WHERE lot='child'")
        before = self.app.projections.manifest()
        with self.assertRaises(ChainError) as caught:
            self.app.projections.verify_rebuild()
        self.assertEqual(caught.exception.code, "integrity")
        self.assertEqual(self.app.projections.manifest(), before)
        with self.assertRaises(ChainError):
            self.app.check()


class StocktakeTests(unittest.TestCase):
    def setUp(self):
        self.app = built(app())
        self.addCleanup(self.app.close)

    def test_balanced_stocktake_closes(self):
        opened = self.app.stocktakes.open("st", "B")
        self.assertEqual(opened["baseline"], {"child": "5"})
        self.assertEqual(opened["revision"], 1)
        counted = self.app.stocktakes.count("st", {"child": "5"}, 1)
        self.assertTrue(counted["balanced"])
        self.assertTrue(counted["stable"])
        closed = self.app.stocktakes.close("st", 2)
        self.assertEqual(closed["state"], "closed")
        self.assertEqual(closed["revision"], 3)
        with self.assertRaises(ChainError):
            self.app.stocktakes.count("st", {"child": "5"}, 3)

    def test_discrepancies_do_not_silently_adjust_stock(self):
        self.app.stocktakes.open("st", "B")
        difference = self.app.stocktakes.count("st", {"child": "4"}, 1)
        self.assertFalse(difference["balanced"])
        self.assertEqual(difference["items"][0]["difference"], "-1")
        self.assertEqual(self.app.graph.stock("child"), 5)
        with self.assertRaises(ChainError):
            self.app.stocktakes.close("st", 2)
        self.assertEqual(self.app.stocktakes.get("st")["state"], "counting")
        self.assertEqual(self.app.graph.stock("child"), 5)

    def test_movement_requires_rebase_and_recount(self):
        self.app.stocktakes.open("st", "B")
        self.app.stocktakes.count("st", {"child": "5"}, 1)
        move = {
            "id": "move",
            "kind": "move",
            "lot": "child",
            "quantity": "1",
            "from": "B",
            "to": "C",
        }
        self.app.consumer.submit(envelope("move", "f", 2, [move], T2))
        differences = self.app.stocktakes.differences("st")
        self.assertFalse(differences["stable"])
        self.assertEqual(differences["items"][0]["movement_since_open"], "-1")
        with self.assertRaises(ChainError):
            self.app.stocktakes.close("st", 2)
        rebased = self.app.stocktakes.rebase("st", 2)
        self.assertIsNone(rebased["counts"])
        self.assertEqual(rebased["baseline"], {"child": "4"})
        self.app.stocktakes.count("st", {"child": "4"}, 3)
        self.assertEqual(self.app.stocktakes.close("st", 4)["state"], "closed")

    def test_counts_cover_baseline_and_revision(self):
        self.app.stocktakes.open("st", "B")
        with self.assertRaises(ChainError):
            self.app.stocktakes.count("st", {}, 1)
        with self.assertRaises(ChainError):
            self.app.stocktakes.count("st", {"child": "5"}, 2)
        with self.assertRaises(ChainError):
            self.app.stocktakes.count("st", {"child": "5", "missing": "1"}, 1)
        with self.assertRaises(ChainError):
            self.app.stocktakes.open("st", "C")
        self.assertEqual(self.app.stocktakes.get("st")["revision"], 1)
        self.assertIsNone(self.app.stocktakes.get("st")["counts"])


class RecoveryTests(unittest.TestCase):
    def test_persistence_backup_and_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "s.sqlite3"
            backup = Path(directory) / "b.sqlite3"
            with built(app(path)) as original:
                original.recalls.open("r", "raw", "Review")
                original.journal.backup(backup)
                manifest = original.projections.manifest()
            with SupplyChain(path) as reopened:
                self.assertEqual(reopened.projections.manifest(), manifest)
                self.assertEqual(reopened.graph.stock("child"), 5)
                self.assertEqual(reopened.recalls.get("r")["state"], "open")
                self.assertTrue(reopened.projections.verify_rebuild()["matching"])
            with SupplyChain(backup) as restored:
                self.assertEqual(restored.projections.manifest(), manifest)
                self.assertTrue(restored.graph.blocked("child"))
                self.assertTrue(restored.check()["conservation"]["conserved"])

    def test_concurrent_duplicate_event(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "s.sqlite3"
            with app(path):
                pass
            apps = [SupplyChain(path), SupplyChain(path)]
            barrier = threading.Barrier(2)
            outputs = []
            errors = []

            def submit(instance):
                try:
                    barrier.wait()
                    outputs.append(instance.consumer.submit(envelope()))
                except Exception as exc:
                    errors.append(exc)

            threads = [
                threading.Thread(target=submit, args=(instance,)) for instance in apps
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(errors, [])
            self.assertEqual(
                sorted(item["duplicate"] for item in outputs), [False, True]
            )
            self.assertEqual(apps[0].graph.stock("raw"), 10)
            self.assertEqual(apps[0].consumer.gaps("s")["next_offset"], 1)
            for instance in apps:
                instance.close()

    def test_nested_transaction_rolls_back_inner_only(self):
        with app() as instance:
            with instance.journal.transaction() as con:
                con.execute("INSERT INTO metadata VALUES('outer','yes')")
                with self.assertRaises(RuntimeError):
                    with instance.journal.transaction() as inner:
                        inner.execute("INSERT INTO metadata VALUES('inner','no')")
                        raise RuntimeError("synthetic rollback")
                self.assertIsNone(
                    instance.journal.one("SELECT * FROM metadata WHERE key='inner'")
                )
            self.assertEqual(
                instance.journal.one("SELECT value FROM metadata WHERE key='outer'")[
                    "value"
                ],
                "yes",
            )
            self.assertTrue(instance.check()["conservation"]["conserved"])

    def test_cli_demo_and_actions(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(["demo"])
        self.assertEqual(code, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result["initial_state"], "blocked")
        self.assertEqual(result["resolved_state"], "applied")
        self.assertEqual(result["conservation"]["materials"][0]["waste"], "1")
        with app() as instance:
            self.assertEqual(dispatch(instance, {"action": "pending"}), [])
            self.assertTrue(
                dispatch(instance, {"action": "check"})["conservation"]["conserved"]
            )
            with self.assertRaises(ChainError):
                dispatch(instance, {"action": "unknown"})

    def test_cli_reports_invalid_request(self):
        with tempfile.TemporaryDirectory() as directory:
            request = Path(directory) / "request.json"
            request.write_text('{"action":"unknown"}')
            error = io.StringIO()
            with redirect_stderr(error):
                result = main(
                    ["--db", str(Path(directory) / "s.sqlite3"), "run", str(request)]
                )
            self.assertEqual(result, 2)
            payload = json.loads(error.getvalue())
            self.assertEqual(payload["error"]["code"], "input")
            self.assertIn("unknown", payload["error"]["message"])

    def test_audit_and_event_digest_tampering(self):
        with built(app()) as instance:
            with instance.journal.transaction() as con:
                con.execute("UPDATE envelopes SET digest='bad' WHERE id='e0'")
            with self.assertRaises(ChainError) as caught:
                instance.check()
            self.assertEqual(caught.exception.code, "integrity")
        with app() as instance:
            with instance.journal.transaction() as con:
                con.execute("UPDATE audit SET payload='{}' WHERE sequence=1")
            with self.assertRaises(ChainError):
                instance.check()
