from decimal import Decimal
from ..types import (
    key,
    obj,
    array,
    integer,
    amount,
    text,
    canonical,
    decode,
    fingerprint,
    fail,
)


class QualityRelease:
    def __init__(self, journal, graph):
        self.journal = journal
        self.graph = graph
        with journal.transaction() as con:
            con.execute(
                """CREATE TABLE IF NOT EXISTS inspection_specs(id TEXT PRIMARY KEY,payload TEXT NOT NULL,digest TEXT NOT NULL)"""
            )
            con.execute("""CREATE TABLE IF NOT EXISTS inspections(id TEXT PRIMARY KEY,lot TEXT NOT NULL,spec TEXT NOT NULL,
                state TEXT NOT NULL,revision INTEGER NOT NULL,baseline TEXT NOT NULL,result TEXT)""")
            con.execute("""CREATE TABLE IF NOT EXISTS inspection_samples(inspection TEXT NOT NULL,id TEXT NOT NULL,
                payload TEXT NOT NULL,PRIMARY KEY(inspection,id))""")
            con.execute("""CREATE TABLE IF NOT EXISTS quality_dispositions(id TEXT PRIMARY KEY,inspection TEXT NOT NULL,
                payload TEXT NOT NULL)""")

    def specification(self, data):
        obj(
            data,
            {"id", "material", "minimum_samples", "maximum_defects", "criteria"},
            {"id", "material", "minimum_samples", "maximum_defects", "criteria"},
        )
        spec_id = key(data["id"])
        material = key(data["material"])
        minimum = integer(data["minimum_samples"], "minimum_samples", 1, 10000)
        defects = integer(data["maximum_defects"], "maximum_defects", 0, minimum)
        criteria = []
        names = set()
        for item in array(data["criteria"], 1, 100):
            obj(
                item,
                {"name", "unit", "lower", "upper"},
                {"name", "unit", "lower", "upper"},
            )
            name = key(item["name"])
            if name in names:
                fail("input", "duplicate inspection criterion")
            names.add(name)
            lower = amount(item["lower"], allow_zero=True)
            upper = amount(item["upper"], allow_zero=True)
            if lower > upper:
                fail("input", "inspection limits must increase")
            criteria.append(
                {
                    "name": name,
                    "unit": key(item["unit"]),
                    "lower": text(lower),
                    "upper": text(upper),
                }
            )
        criteria.sort(key=lambda item: item["name"])
        payload = {
            "id": spec_id,
            "material": material,
            "minimum_samples": minimum,
            "maximum_defects": defects,
            "criteria": criteria,
        }
        with self.journal.transaction() as con:
            old = self.journal.one(
                "SELECT * FROM inspection_specs WHERE id=?", (spec_id,)
            )
            if old:
                if old["digest"] != fingerprint(payload):
                    fail("conflict", "inspection specification is immutable")
                return payload
            con.execute(
                "INSERT INTO inspection_specs VALUES(?,?,?)",
                (spec_id, canonical(payload), fingerprint(payload)),
            )
            self.journal.record("inspection_spec", spec_id, payload)
        return payload

    def spec(self, spec_id):
        row = self.journal.one(
            "SELECT payload FROM inspection_specs WHERE id=?", (spec_id,)
        )
        if row is None:
            fail("missing", "inspection specification not found")
        return decode(row["payload"])

    def open(self, inspection_id, lot, spec_id):
        key(inspection_id)
        with self.journal.transaction() as con:
            existing = self.journal.one(
                "SELECT * FROM inspections WHERE id=?", (inspection_id,)
            )
            if existing:
                if existing["lot"] != lot or existing["spec"] != spec_id:
                    fail("conflict", "inspection identity differs")
                return self.get(inspection_id)
            record = self.graph.lot(lot)
            spec = self.spec(spec_id)
            if spec["material"] != record["material"]:
                fail("material", "inspection specification material differs")
            baseline = fingerprint(record)
            con.execute(
                "INSERT INTO inspections VALUES(?,?,?,'sampling',1,?,NULL)",
                (inspection_id, lot, spec_id, baseline),
            )
            hold_id = "inspection:" + inspection_id
            if self.journal.one("SELECT id FROM holds WHERE id=?", (hold_id,)):
                fail("conflict", "inspection hold identity collides")
            con.execute(
                "INSERT INTO holds VALUES(?,?,?,1)",
                (hold_id, lot, "Inspection awaiting disposition"),
            )
            self.journal.record(
                "inspection_opened",
                inspection_id,
                {"lot": lot, "spec": spec_id, "baseline": baseline},
            )
        return self.get(inspection_id)

    def get(self, inspection_id):
        row = self.journal.one("SELECT * FROM inspections WHERE id=?", (inspection_id,))
        if row is None:
            fail("missing", "inspection not found")
        return {
            **row,
            "result": decode(row["result"]) if row["result"] else None,
            "samples": [
                decode(item["payload"])
                for item in self.journal.rows(
                    "SELECT payload FROM inspection_samples WHERE inspection=? ORDER BY id",
                    (inspection_id,),
                )
            ],
        }

    def sample(self, inspection_id, sample_id, measurements, expected_revision):
        key(sample_id)
        obj(measurements)
        with self.journal.transaction() as con:
            inspection = self.get(inspection_id)
            if (
                inspection["state"] != "sampling"
                or inspection["revision"] != expected_revision
            ):
                fail("conflict", "inspection state or revision changed")
            spec = self.spec(inspection["spec"])
            names = {item["name"] for item in spec["criteria"]}
            if set(measurements) != names:
                fail("input", "sample must cover exactly specified criteria")
            normalized = {}
            failures = []
            for criterion in spec["criteria"]:
                item = measurements[criterion["name"]]
                obj(item, {"value", "unit"}, {"value", "unit"})
                if item["unit"] != criterion["unit"]:
                    fail("unit", "sample unit differs from inspection specification")
                value = amount(item["value"], allow_zero=True)
                normalized[criterion["name"]] = {
                    "value": text(value),
                    "unit": item["unit"],
                }
                if (
                    not Decimal(criterion["lower"])
                    <= value
                    <= Decimal(criterion["upper"])
                ):
                    failures.append(criterion["name"])
            payload = {
                "id": sample_id,
                "measurements": normalized,
                "failed_criteria": failures,
            }
            old = self.journal.one(
                "SELECT payload FROM inspection_samples WHERE inspection=? AND id=?",
                (inspection_id, sample_id),
            )
            if old:
                if old["payload"] != canonical(payload):
                    fail("conflict", "sample id content differs")
                return self.get(inspection_id)
            if len(inspection["samples"]) >= 10000:
                fail("limit", "inspection sample limit reached")
            con.execute(
                "INSERT INTO inspection_samples VALUES(?,?,?)",
                (inspection_id, sample_id, canonical(payload)),
            )
            con.execute(
                "UPDATE inspections SET revision=revision+1 WHERE id=?",
                (inspection_id,),
            )
            self.journal.record("inspection_sample", inspection_id, payload)
        return self.get(inspection_id)

    def evaluate(self, inspection_id):
        inspection = self.get(inspection_id)
        spec = self.spec(inspection["spec"])
        defects = [
            sample["id"]
            for sample in inspection["samples"]
            if sample["failed_criteria"]
        ]
        observed = len(inspection["samples"])
        unchanged = (
            fingerprint(self.graph.lot(inspection["lot"])) == inspection["baseline"]
        )
        return {
            "id": inspection_id,
            "observed": observed,
            "required": spec["minimum_samples"],
            "defects": defects,
            "maximum_defects": spec["maximum_defects"],
            "baseline_unchanged": unchanged,
            "passed": observed >= spec["minimum_samples"]
            and len(defects) <= spec["maximum_defects"]
            and unchanged,
            "sample_digest": fingerprint(inspection["samples"]),
        }

    def disposition(
        self,
        disposition_id,
        inspection_id,
        expected_revision,
        reviewer,
        release,
        reason,
    ):
        key(disposition_id)
        key(reviewer)
        if (
            not isinstance(release, bool)
            or not isinstance(reason, str)
            or not reason.strip()
        ):
            fail("input", "disposition needs explicit release decision and reason")
        with self.journal.transaction() as con:
            inspection = self.get(inspection_id)
            existing = self.journal.one(
                "SELECT * FROM quality_dispositions WHERE id=?", (disposition_id,)
            )
            if existing:
                previous = decode(existing["payload"])
                if existing["inspection"] != inspection_id or (
                    previous["reviewer"],
                    previous["release"],
                    previous["reason"],
                ) != (reviewer, release, reason):
                    fail("conflict", "disposition identity differs")
                return previous
            if (
                inspection["state"] != "sampling"
                or inspection["revision"] != expected_revision
            ):
                fail("conflict", "inspection state or revision changed")
            assessment = self.evaluate(inspection_id)
            if release and not assessment["passed"]:
                fail("quality", "inspection cannot be released", assessment=assessment)
            payload = {
                "id": disposition_id,
                "inspection": inspection_id,
                "reviewer": reviewer,
                "release": release,
                "reason": reason,
                "assessment": assessment,
            }
            con.execute(
                "INSERT INTO quality_dispositions VALUES(?,?,?)",
                (disposition_id, inspection_id, canonical(payload)),
            )
            con.execute(
                "UPDATE inspections SET state=?,revision=revision+1,result=? WHERE id=?",
                (
                    "released" if release else "rejected",
                    canonical(payload),
                    inspection_id,
                ),
            )
            if release:
                con.execute(
                    "UPDATE holds SET active=0 WHERE id=?",
                    ("inspection:" + inspection_id,),
                )
            self.journal.record("inspection_disposition", disposition_id, payload)
        return payload

    def supplier_summary(self, supplier):
        rows = self.journal.rows(
            """SELECT i.id,i.state,l.material FROM inspections i JOIN lots l ON l.id=i.lot
            WHERE l.supplier=? ORDER BY i.id""",
            (supplier,),
        )
        summary = {}
        for row in rows:
            group = summary.setdefault(
                row["material"],
                {
                    "sampling": 0,
                    "released": 0,
                    "rejected": 0,
                    "defective_samples": 0,
                    "samples": 0,
                },
            )
            group[row["state"]] += 1
            assessment = self.evaluate(row["id"])
            group["defective_samples"] += len(assessment["defects"])
            group["samples"] += assessment["observed"]
        return {"supplier": supplier, "materials": summary}
