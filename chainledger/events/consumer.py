from ..types import key, integer, canonical, decode, fail, ChainError
from .protocol import Envelope


class Consumer:
    def __init__(self, journal, graph):
        self.journal = journal
        self.graph = graph

    def register(self, source, start_offset=0):
        key(source)
        integer(start_offset, "start_offset")
        with self.journal.transaction() as con:
            row = con.execute(
                "SELECT next_offset FROM sources WHERE id=?", (source,)
            ).fetchone()
            if row:
                return {
                    "source": source,
                    "next_offset": row["next_offset"],
                    "existing": True,
                }
            con.execute("INSERT INTO sources VALUES(?,?)", (source, start_offset))
            self.journal.record(
                "source_registered", source, {"start_offset": start_offset}
            )
        return {"source": source, "next_offset": start_offset, "existing": False}

    def submit(self, data, drain=True):
        envelope = Envelope(data)
        with self.journal.transaction() as con:
            if not con.execute(
                "SELECT id FROM sources WHERE id=?", (envelope.source,)
            ).fetchone():
                fail("source", "source must be registered")
            old = con.execute(
                "SELECT * FROM envelopes WHERE id=?", (envelope.id,)
            ).fetchone()
            if old:
                if old["digest"] != envelope.digest:
                    fail("conflict", "event identity reused with different payload")
                return {**self.status(envelope.id), "duplicate": True}
            collision = con.execute(
                "SELECT id FROM envelopes WHERE source=? AND offset=?",
                (envelope.source, envelope.offset),
            ).fetchone()
            if collision:
                fail(
                    "conflict",
                    "source offset already occupied",
                    existing=collision["id"],
                )
            offset = con.execute(
                "SELECT next_offset FROM sources WHERE id=?", (envelope.source,)
            ).fetchone()[0]
            if envelope.offset < offset:
                fail("offset", "event offset precedes acknowledged position")
            con.execute(
                "INSERT INTO envelopes VALUES(?,?,?,?,?,'pending',NULL,NULL)",
                (
                    envelope.id,
                    envelope.source,
                    envelope.offset,
                    canonical(envelope.as_dict()),
                    envelope.digest,
                ),
            )
            self.journal.record(
                "event_received",
                envelope.id,
                {
                    "source": envelope.source,
                    "offset": envelope.offset,
                    "digest": envelope.digest,
                },
            )
        if drain:
            self.drain()
        return {**self.status(envelope.id), "duplicate": False}

    def drain(self, maximum=10000):
        integer(maximum, "maximum", 1, 100000)
        applied = []
        rejected = []
        while len(applied) + len(rejected) < maximum:
            progressed = False
            with self.journal.transaction() as con:
                candidates = self.journal.rows("""SELECT e.* FROM envelopes e JOIN sources s ON s.id=e.source
                    WHERE e.offset=s.next_offset AND e.state IN ('pending','blocked') ORDER BY e.source,e.offset""")
                for row in candidates:
                    if len(applied) + len(rejected) >= maximum:
                        break
                    envelope = Envelope(decode(row["payload"]))
                    try:
                        with self.journal.transaction():
                            self.graph.apply(envelope)
                    except ChainError as exc:
                        state = "blocked" if exc.code == "dependency" else "rejected"
                        con.execute(
                            "UPDATE envelopes SET state=?,reason=? WHERE id=?",
                            (state, canonical(exc.as_dict()), envelope.id),
                        )
                        if state == "rejected":
                            rejected.append(envelope.id)
                            self.journal.record(
                                "event_rejected", envelope.id, exc.as_dict()
                            )
                            progressed = True
                        continue
                    order = self.journal.one(
                        "SELECT MAX(applied_order) AS n FROM envelopes"
                    )["n"]
                    order = (order or 0) + 1
                    con.execute(
                        "UPDATE envelopes SET state='applied',reason=NULL,applied_order=? WHERE id=?",
                        (order, envelope.id),
                    )
                    con.execute(
                        "UPDATE sources SET next_offset=next_offset+1 WHERE id=?",
                        (envelope.source,),
                    )
                    self.journal.record("event_applied", envelope.id, {"order": order})
                    applied.append(envelope.id)
                    progressed = True
            if not progressed:
                break
        return {"applied": applied, "rejected": rejected, "pending": self.pending()}

    def status(self, event_id):
        row = self.journal.one("SELECT * FROM envelopes WHERE id=?", (event_id,))
        if row is None:
            fail("missing", "event not found")
        return {
            "id": row["id"],
            "source": row["source"],
            "offset": row["offset"],
            "state": row["state"],
            "reason": decode(row["reason"]) if row["reason"] else None,
            "applied_order": row["applied_order"],
        }

    def pending(self):
        rows = self.journal.rows(
            "SELECT id FROM envelopes WHERE state!='applied' AND state!='skipped' ORDER BY source,offset"
        )
        return [self.status(row["id"]) for row in rows]

    def skip(self, event_id, reason):
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 512:
            fail("input", "skip requires a bounded explanation")
        with self.journal.transaction() as con:
            row = self.journal.one("SELECT * FROM envelopes WHERE id=?", (event_id,))
            if row is None:
                fail("missing", "event not found")
            source = self.journal.one(
                "SELECT next_offset FROM sources WHERE id=?", (row["source"],)
            )
            if (
                row["state"] not in {"blocked", "rejected"}
                or row["offset"] != source["next_offset"]
            ):
                fail("state", "only blocked or rejected head event may be skipped")
            con.execute(
                "UPDATE envelopes SET state='skipped',reason=? WHERE id=?",
                (canonical({"reason": reason}), event_id),
            )
            con.execute(
                "UPDATE sources SET next_offset=next_offset+1 WHERE id=?",
                (row["source"],),
            )
            self.journal.record("event_skipped", event_id, {"reason": reason})
        return self.drain()

    def gaps(self, source):
        row = self.journal.one("SELECT next_offset FROM sources WHERE id=?", (source,))
        if row is None:
            fail("missing", "source not found")
        pending = self.journal.rows(
            "SELECT offset FROM envelopes WHERE source=? AND offset>=? ORDER BY offset",
            (source, row["next_offset"]),
        )
        cursor = row["next_offset"]
        gaps = []
        for item in pending:
            if item["offset"] > cursor:
                gaps.append({"first": cursor, "last": item["offset"] - 1})
            cursor = item["offset"] + 1
        return {"source": source, "next_offset": row["next_offset"], "gaps": gaps}
