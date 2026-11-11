from .types import key, canonical, decode, fingerprint, fail
from .events.protocol import Envelope

TABLES = (
    "balances",
    "edges",
    "inputs",
    "outputs",
    "transformations",
    "shipments",
    "holds",
    "scraps",
    "operations",
    "lots",
)


class Projections:
    def __init__(self, journal, graph):
        self.journal = journal
        self.graph = graph

    def manifest(self):
        result = []
        for table in TABLES:
            rows = self.journal.rows("SELECT * FROM " + table)
            rows.sort(key=canonical)
            result.append(
                {"table": table, "rows": len(rows), "digest": fingerprint(rows)}
            )
        return {"tables": result, "digest": fingerprint(result)}

    def rebuild(self, replacement=None):
        applied = self.journal.rows(
            "SELECT * FROM envelopes WHERE state='applied' ORDER BY applied_order"
        )
        replacements = {}
        for row in self.journal.rows(
            "SELECT target,replacement FROM adjustments ORDER BY rowid"
        ):
            replacements[row["target"]] = decode(row["replacement"])
        if replacement:
            replacements.update(replacement)
        for table in TABLES:
            self.journal.connection.execute("DELETE FROM " + table)
        self.graph.replaying = True
        try:
            for row in applied:
                data = replacements.get(row["id"], decode(row["payload"]))
                self.graph.apply(Envelope(data))
        finally:
            self.graph.replaying = False
        for row in self.journal.rows("SELECT id,payload FROM recalls ORDER BY id"):
            recall = decode(row["payload"])
            self.graph.lot(recall["root"])
            self.journal.connection.execute(
                "INSERT INTO holds VALUES(?,?,?,1)",
                ("recall:" + row["id"], recall["root"], recall["reason"]),
            )
        for row in self.journal.rows(
            "SELECT id,lot,state FROM inspections ORDER BY id"
        ):
            self.graph.lot(row["lot"])
            self.journal.connection.execute(
                "INSERT INTO holds VALUES(?,?,?,?)",
                (
                    "inspection:" + row["id"],
                    row["lot"],
                    "Inspection awaiting disposition",
                    int(row["state"] != "released"),
                ),
            )
        self.graph.conservation()
        return self.manifest()

    def verify_rebuild(self):
        before = self.manifest()
        try:
            with self.journal.transaction():
                after = self.rebuild()
                if before != after:
                    fail("integrity", "material projection differs from journal replay")
                raise _Rollback(before)
        except _Rollback as result:
            return {"matching": True, **result.result}

    def correct(self, correction_id, target, replacement, reason):
        key(correction_id)
        key(target)
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 512:
            fail("input", "correction requires an explanation")
        envelope = Envelope(replacement)
        with self.journal.transaction() as con:
            existing = self.journal.one(
                "SELECT * FROM adjustments WHERE id=?", (correction_id,)
            )
            if existing:
                if (
                    existing["target"] != target
                    or existing["replacement"] != canonical(envelope.as_dict())
                    or existing["reason"] != reason
                ):
                    fail("conflict", "correction identity differs")
                return {**decode(existing["result"]), "reused": True}
            original = self.journal.one(
                "SELECT * FROM envelopes WHERE id=? AND state='applied'", (target,)
            )
            if original is None:
                fail("state", "correction requires applied target")
            if (envelope.id, envelope.source, envelope.offset) != (
                original["id"],
                original["source"],
                original["offset"],
            ):
                fail("conflict", "correction must retain envelope identity and offset")
            before = self.manifest()
            after = self.rebuild({target: envelope.as_dict()})
            result = {
                "id": correction_id,
                "target": target,
                "before": before["digest"],
                "after": after["digest"],
                "reused": False,
            }
            con.execute(
                "INSERT INTO adjustments VALUES(?,?,?,?,?)",
                (
                    correction_id,
                    target,
                    canonical(envelope.as_dict()),
                    reason,
                    canonical(result),
                ),
            )
            self.journal.record("projection_corrected", correction_id, result)
        return result

    def checkpoint(self, checkpoint_id):
        key(checkpoint_id)
        with self.journal.transaction() as con:
            payload = {
                "projection": self.manifest(),
                "sources": self.journal.rows("SELECT * FROM sources ORDER BY id"),
                "events": self.journal.rows(
                    "SELECT id,digest,state,applied_order FROM envelopes ORDER BY id"
                ),
                "adjustments": self.journal.rows(
                    "SELECT id,target FROM adjustments ORDER BY id"
                ),
            }
            digest = fingerprint(payload)
            old = con.execute(
                "SELECT digest FROM checkpoints WHERE id=?", (checkpoint_id,)
            ).fetchone()
            if old:
                if old["digest"] != digest:
                    fail("conflict", "checkpoint id has different state")
                return {"id": checkpoint_id, "digest": digest, "reused": True}
            con.execute(
                "INSERT INTO checkpoints VALUES(?,?,?)",
                (checkpoint_id, canonical(payload), digest),
            )
            self.journal.record("checkpoint_created", checkpoint_id, {"digest": digest})
        return {"id": checkpoint_id, "digest": digest, "reused": False}

    def compare_checkpoint(self, checkpoint_id):
        row = self.journal.one("SELECT * FROM checkpoints WHERE id=?", (checkpoint_id,))
        if row is None:
            fail("missing", "checkpoint not found")
        payload = decode(row["payload"])
        if fingerprint(payload) != row["digest"]:
            fail("integrity", "checkpoint corrupted")
        current = self.manifest()
        before = {item["table"]: item for item in payload["projection"]["tables"]}
        changed = [
            {"table": item["table"], "before": before[item["table"]], "after": item}
            for item in current["tables"]
            if before[item["table"]] != item
        ]
        return {"id": checkpoint_id, "same_projection": not changed, "changes": changed}


class _Rollback(Exception):
    def __init__(self, result):
        self.result = result
