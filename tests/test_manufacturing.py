import json
import tempfile
import unittest
from pathlib import Path

from chainledger.events.manufacturing import (
    ManufacturingExchange,
    parse_xml,
    validate_exchange,
    validate_mapping,
)
from chainledger.types import ChainError, canonical
from helpers import app

MAPPING = {
    "id": "codes-1",
    "revision": 1,
    "materials": {"M-01": "alloy"},
    "locations": {"L-A": "A", "L-B": "B"},
    "units": {
        "kg": {"unit": "kg", "factor": "1"},
        "g": {"unit": "kg", "factor": "0.001"},
        "L": {"unit": "litre", "factor": "1"},
    },
}

RECEIVE = """<?xml version="1.0" encoding="UTF-8"?>
<Exchange xmlns="urn:manufacturing:1" source="plant" mapping="codes-1">
 <Transaction id="r1" sequence="0" at="2026-11-11T00:00:00Z">
  <Receive id="in1" lot="raw" material="M-01" location="L-A" supplier="factory">
   <Quantity unit="g">10000</Quantity>
  </Receive>
 </Transaction>
</Exchange>
"""

TRANSFORM = """<?xml version="1.0" encoding="UTF-8"?>
<Exchange xmlns="urn:manufacturing:2" source="plant" mapping="codes-1">
 <Transaction id="t1" sequence="1" at="2026-11-11T01:00:00Z">
  <Transform id="cut1" process="cut">
   <Input lot="raw"><Quantity unit="kg">10</Quantity></Input>
   <Output lot="child" location="L-B"><Quantity unit="g">9000</Quantity></Output>
   <Waste><Quantity unit="kg">1</Quantity></Waste>
  </Transform>
 </Transaction>
</Exchange>
"""


class XmlSafetyTests(unittest.TestCase):
    def parse_fails(self, content, code="xml"):
        with self.assertRaises(ChainError) as caught:
            parse_xml(content)
        self.assertEqual(caught.exception.code, code)

    def test_doctype_and_entities_rejected(self):
        self.parse_fails(
            '<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "b">]>'
            '<x xmlns="urn:manufacturing:1"/>'
        )
        self.parse_fails(
            '<?xml version="1.0"?><!DOCTYPE Exchange SYSTEM "http://example.invalid/x">'
            '<Exchange xmlns="urn:manufacturing:1"/>'
        )

    def test_internal_entity_expansion_blocked(self):
        self.parse_fails(
            '<?xml version="1.0"?><!DOCTYPE Exchange [<!ENTITY x "abc">]>'
            '<Exchange xmlns="urn:manufacturing:1">&x;</Exchange>'
        )

    def test_undeclared_namespace_and_unknown_structure(self):
        self.parse_fails(
            '<Exchange xmlns="urn:manufacturing:3" source="p" mapping="m"/>'
        )
        root = parse_xml(
            '<Exchange xmlns="urn:manufacturing:1" source="p" mapping="m">'
            '<Transaction id="t" sequence="0" at="2026-11-11T00:00:00Z">'
            '<Unknown id="u"/></Transaction></Exchange>'
        )
        with self.assertRaises(ChainError) as caught:
            validate_exchange(root)
        self.assertEqual(caught.exception.code, "xml")

    def test_processing_instruction_and_unqualified_element(self):
        self.parse_fails(
            '<?xml version="1.0"?><?stylesheet type="text/xsl" href="x"?>'
            '<Exchange xmlns="urn:manufacturing:1" source="p" mapping="m"/>'
        )
        self.parse_fails('<Exchange source="p" mapping="m"/>')

    def test_unknown_attribute_rejected(self):
        content = RECEIVE.replace(
            '<Receive id="in1"', '<Receive unexpected="no" id="in1"'
        )
        root = parse_xml(content)
        with self.assertRaises(ChainError) as caught:
            validate_exchange(root)
        self.assertEqual(caught.exception.code, "xml")

    def test_malformed_and_bounded_decimal(self):
        self.parse_fails("<Exchange")
        bad = RECEIVE.replace(">10000<", ">1e4<")
        root = parse_xml(bad)
        with self.assertRaises(ChainError):
            validate_exchange(root)


class MappingTests(unittest.TestCase):
    def test_revision_immutability_and_validation(self):
        mapping = validate_mapping(dict(MAPPING))
        self.assertEqual(mapping["units"]["g"]["factor"], "0.001")
        with self.assertRaises(ChainError):
            validate_mapping({**MAPPING, "revision": -1})
        with self.assertRaises(ChainError):
            validate_mapping({**MAPPING, "units": {"g": {"unit": "kg", "factor": "0"}}})
        application = app()
        self.addCleanup(application.close)
        self.assertFalse(
            application.manufacturing.load_mapping(dict(MAPPING))["existing"]
        )
        self.assertTrue(
            application.manufacturing.load_mapping(dict(MAPPING))["existing"]
        )
        changed = {**MAPPING, "materials": {"M-01": "copper"}}
        with self.assertRaises(ChainError) as caught:
            application.manufacturing.load_mapping(changed)
        self.assertEqual(caught.exception.code, "conflict")
        revision_two = {**MAPPING, "revision": 2,
                        "locations": {**MAPPING["locations"], "L-C": "C"}}
        result = application.manufacturing.load_mapping(revision_two)
        self.assertEqual(result["revision"], 2)


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.application = app()
        self.addCleanup(self.application.close)
        self.application.manufacturing.load_mapping(dict(MAPPING))
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def write(self, name, content):
        path = self.directory / name
        path.write_text(content, encoding="utf-8")
        return path

    def test_cross_file_predecessor_retried_and_units_converted(self):
        transform = self.application.manufacturing.ingest_xml(
            self.write("transform.xml", TRANSFORM)
        )
        self.assertEqual(transform["received"], 1)
        view = self.application.manufacturing.transaction("plant", "t1")
        self.assertEqual(view["state"], "pending")
        self.assertEqual(view["mapping"]["revision"], 1)
        self.assertIn("@line3", view["xml_location"])
        receive = self.application.manufacturing.ingest_xml(
            self.write("receive.xml", RECEIVE)
        )
        self.assertEqual(
            receive["drain"]["applied"],
            ["mfg:plant:r1", "mfg:plant:t1"],
        )
        # 10000 g = 10 kg；10 kg 投入 = 9 kg 产出 + 1 kg 损耗
        self.assertEqual(self.application.graph.stock("raw"), 0)
        self.assertEqual(self.application.graph.stock("child"), 9)
        self.assertEqual(
            self.application.trace.path("raw", "child"), ["raw", "child"]
        )
        self.assertTrue(
            self.application.check()["conservation"]["conserved"]
        )

    def test_duplicate_file_is_idempotent(self):
        path = self.write("receive.xml", RECEIVE)
        first = self.application.manufacturing.ingest_xml(path)
        self.assertEqual(first["received"], 1)
        second = self.application.manufacturing.ingest_xml(path)
        self.assertTrue(second["duplicate_file"])
        self.assertEqual(second["received"], 0)
        self.assertEqual(self.application.graph.stock("raw"), 10)

    def test_same_identity_different_content_is_quarantined(self):
        self.application.manufacturing.ingest_xml(self.write("a.xml", RECEIVE))
        mutated = RECEIVE.replace(">10000<", ">11000<")
        result = self.application.manufacturing.ingest_xml(
            self.write("b.xml", mutated)
        )
        self.assertEqual(result["quarantined"], 1)
        self.assertEqual(result["received"], 0)
        quarantine = self.application.manufacturing.quarantine()
        self.assertEqual(len(quarantine), 1)
        self.assertEqual(quarantine[0]["reason"]["code"], "content_conflict")
        self.assertEqual(self.application.graph.stock("raw"), 10)
        # 原始事务状态与材料图保持不变
        self.assertEqual(
            self.application.manufacturing.transaction("plant", "r1")["state"],
            "applied",
        )

    def test_missing_mapping_keeps_repairable_block(self):
        fresh = app()
        self.addCleanup(fresh.close)
        result = fresh.manufacturing.ingest_xml(self.write("r.xml", RECEIVE))
        self.assertEqual(result["blocked"], 1)
        view = fresh.manufacturing.transaction("plant", "r1")
        self.assertEqual(view["adapter_state"], "blocked")
        self.assertEqual(view["reason"]["code"], "mapping_not_loaded")
        self.assertEqual(fresh.inventory.list()["items"], [])
        fresh.manufacturing.load_mapping(dict(MAPPING))
        retried = fresh.manufacturing.reprocess("plant", "r1")
        self.assertEqual(retried["transaction"]["state"], "applied")
        self.assertEqual(fresh.graph.stock("raw"), 10)

    def test_unknown_business_code_is_repairable_but_hard_unknowns_reject_file(self):
        bad_location = RECEIVE.replace('location="L-A"', 'location="L-X"')
        result = self.application.manufacturing.ingest_xml(
            self.write("bad-location.xml", bad_location)
        )
        self.assertEqual(result["blocked"], 1)
        view = self.application.manufacturing.transaction("plant", "r1")
        self.assertEqual(view["reason"]["code"], "unknown_location")
        with self.assertRaises(ChainError) as caught:
            self.application.manufacturing.ingest_xml(
                self.write("xxe.xml",
                           '<!DOCTYPE Exchange [<!ENTITY x SYSTEM "file:///etc/passwd">]>'
                           + RECEIVE)
            )
        self.assertEqual(caught.exception.code, "xml")
        self.assertEqual(
            self.application.journal.one(
                "SELECT COUNT(*) AS n FROM mfg_files"
            )["n"],
            1,
        )

    def test_reprocess_pins_accepted_interpretation(self):
        self.application.manufacturing.ingest_xml(self.write("r.xml", RECEIVE))
        original = self.application.manufacturing.transaction("plant", "r1")
        self.assertEqual(original["mapping"]["revision"], 1)
        revision_two = {**MAPPING, "revision": 2,
                        "materials": {"M-01": "renamed-alloy"}}
        self.application.manufacturing.load_mapping(revision_two)
        again = self.application.manufacturing.ingest_xml(self.write("r2.xml", RECEIVE))
        self.assertTrue(again["duplicate_file"])
        pinned = self.application.manufacturing.transaction("plant", "r1")
        self.assertEqual(pinned["mapping"]["revision"], 1)
        self.assertEqual(self.application.graph.lot("raw")["material"], "alloy")

    def test_unbalanced_transform_is_rejected_as_a_whole(self):
        unbalanced = TRANSFORM.replace(
            '<Waste><Quantity unit="kg">1</Quantity></Waste>',
            '<Waste><Quantity unit="kg">2</Quantity></Waste>',
        )
        self.application.manufacturing.ingest_xml(self.write("r.xml", RECEIVE))
        self.application.manufacturing.ingest_xml(
            self.write("u.xml", unbalanced)
        )
        view = self.application.manufacturing.transaction("plant", "t1")
        self.assertEqual(view["state"], "rejected")
        self.assertEqual(view["reason"]["code"], "conservation")
        with self.assertRaises(ChainError):
            self.application.graph.lot("child")
        # 损耗没有被补平，输入库存未被部分扣减
        self.assertEqual(self.application.graph.stock("raw"), 10)
        self.assertEqual(
            self.application.journal.one(
                "SELECT COUNT(*) AS n FROM transformations"
            )["n"],
            0,
        )

    def test_reprocess_requires_blocked_state(self):
        self.application.manufacturing.ingest_xml(self.write("r.xml", RECEIVE))
        with self.assertRaises(ChainError) as caught:
            self.application.manufacturing.reprocess("plant", "r1")
        self.assertEqual(caught.exception.code, "state")

    def test_unknown_unit_blocks_without_graph_change(self):
        content = RECEIVE.replace('unit="g"', 'unit="stone"')
        result = self.application.manufacturing.ingest_xml(
            self.write("unit.xml", content)
        )
        self.assertEqual(result["blocked"], 1)
        self.assertEqual(
            self.application.manufacturing.transaction("plant", "r1")
            ["reason"]["code"],
            "unknown_unit",
        )
        self.assertEqual(self.application.inventory.list()["items"], [])

    def test_canonical_unit_mismatch_blocks_whole_transform(self):
        content = TRANSFORM.replace(
            '<Output lot="child" location="L-B">'
            '<Quantity unit="g">9000</Quantity></Output>',
            '<Output lot="child" location="L-B">'
            '<Quantity unit="L">9</Quantity></Output>',
        )
        result = self.application.manufacturing.ingest_xml(
            self.write("mismatch.xml", content)
        )
        self.assertEqual(result["blocked"], 1)
        view = self.application.manufacturing.transaction("plant", "t1")
        self.assertEqual(view["reason"]["code"], "unit_mismatch")
        self.assertEqual(
            self.application.inventory.list()["items"], []
        )

    def test_overlong_identity_is_rejected_before_persistence(self):
        content = RECEIVE.replace('id="r1"', 'id="' + "r" * 100 + '"')
        with self.assertRaises(ChainError) as caught:
            self.application.manufacturing.ingest_xml(
                self.write("long.xml", content)
            )
        self.assertEqual(caught.exception.code, "input")
        self.assertEqual(
            self.application.journal.one(
                "SELECT COUNT(*) AS n FROM mfg_transactions"
            )["n"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
