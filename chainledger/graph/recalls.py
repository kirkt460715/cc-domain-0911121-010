from decimal import Decimal
from ..types import (
    key,
    amount,
    text,
    integer,
    canonical,
    decode,
    fingerprint,
    fail,
    timestamp,
)


class Recalls:
    def __init__(self, journal, graph, trace):
        self.journal = journal
        self.graph = graph
        self.trace = trace

    def open(self, recall_id, root, reason):
        key(recall_id)
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 512:
            fail("input", "recall requires a bounded reason")
        with self.journal.transaction() as con:
            impact = self.trace.impact(root)
            payload = {
                "id": recall_id,
                "root": root,
                "reason": reason,
                "impact": impact,
                "impact_digest": fingerprint(impact),
            }
            old = self.journal.one("SELECT * FROM recalls WHERE id=?", (recall_id,))
            if old:
                previous = decode(old["payload"])
                if previous["root"] != root or previous["reason"] != reason:
                    fail("conflict", "recall identity differs")
                return self.get(recall_id)
            con.execute(
                "INSERT INTO recalls VALUES(?,?,'open',1)",
                (recall_id, canonical(payload)),
            )
            hold_id = "recall:" + recall_id
            if self.journal.one("SELECT id FROM holds WHERE id=?", (hold_id,)):
                fail("conflict", "recall hold id collides")
            con.execute("INSERT INTO holds VALUES(?,?,?,1)", (hold_id, root, reason))
            self.journal.record("recall_opened", recall_id, payload)
        return self.get(recall_id)

    def get(self, recall_id):
        row = self.journal.one("SELECT * FROM recalls WHERE id=?", (recall_id,))
        if row is None:
            fail("missing", "recall not found")
        acknowledgements = self.journal.rows(
            "SELECT customer,payload FROM acknowledgements WHERE recall=? ORDER BY customer",
            (recall_id,),
        )
        return {
            **decode(row["payload"]),
            "state": row["state"],
            "revision": row["revision"],
            "acknowledgements": [decode(item["payload"]) for item in acknowledgements],
        }

    def acknowledge(self, recall_id, customer, allocations, expected_revision):
        key(customer)
        integer(expected_revision, "expected_revision", 1)
        if not isinstance(allocations, dict) or not allocations:
            fail("input", "acknowledgement requires per-shipment quantities")
        allocations = {
            key(shipment): text(amount(quantity, allow_zero=True))
            for shipment, quantity in allocations.items()
        }
        with self.journal.transaction() as con:
            recall = self.get(recall_id)
            if recall["state"] != "open" or recall["revision"] != expected_revision:
                fail("conflict", "recall state or revision changed")
            shipments = {
                item["id"]: item
                for item in recall["impact"]["shipments"]
                if item["customer"] == customer
            }
            if not shipments or set(allocations) != set(shipments):
                fail(
                    "input",
                    "acknowledgement must cover exactly this customer affected shipments",
                )
            for shipment, quantity in allocations.items():
                if Decimal(quantity) > Decimal(shipments[shipment]["outstanding"]):
                    fail(
                        "conservation",
                        "acknowledged quantity exceeds affected shipment",
                    )
            payload = {
                "customer": customer,
                "allocations": allocations,
                "impact_digest": recall["impact_digest"],
            }
            con.execute(
                "INSERT INTO acknowledgements VALUES(?,?,?) ON CONFLICT(recall,customer) DO UPDATE SET payload=excluded.payload",
                (recall_id, customer, canonical(payload)),
            )
            con.execute(
                "UPDATE recalls SET revision=revision+1 WHERE id=?", (recall_id,)
            )
            self.journal.record("recall_acknowledged", recall_id, payload)
        return self.get(recall_id)

    def refresh(self, recall_id, expected_revision):
        with self.journal.transaction() as con:
            recall = self.get(recall_id)
            if recall["state"] != "open" or recall["revision"] != expected_revision:
                fail("conflict", "recall state or revision changed")
            impact = self.trace.impact(recall["root"])
            if fingerprint(impact) == recall["impact_digest"]:
                return recall
            payload = {name: recall[name] for name in ("id", "root", "reason")}
            payload.update(impact=impact, impact_digest=fingerprint(impact))
            con.execute(
                "UPDATE recalls SET payload=?,revision=revision+1 WHERE id=?",
                (canonical(payload), recall_id),
            )
            con.execute("DELETE FROM acknowledgements WHERE recall=?", (recall_id,))
            self.journal.record(
                "recall_refreshed",
                recall_id,
                {"impact_digest": payload["impact_digest"]},
            )
        return self.get(recall_id)

    def close(self, recall_id, expected_revision):
        with self.journal.transaction() as con:
            recall = self.get(recall_id)
            if recall["state"] != "open" or recall["revision"] != expected_revision:
                fail("conflict", "recall state or revision changed")
            impact = self.trace.impact(recall["root"])
            if fingerprint(impact) != recall["impact_digest"]:
                fail("conflict", "affected inventory changed; refresh recall first")
            acknowledged = {
                item["customer"]: item for item in recall["acknowledgements"]
            }
            for shipment in impact["shipments"]:
                customer = acknowledged.get(shipment["customer"])
                if customer is None or Decimal(
                    customer["allocations"][shipment["id"]]
                ) != Decimal(shipment["outstanding"]):
                    fail(
                        "state",
                        "all outstanding affected quantities must be acknowledged",
                    )
            con.execute(
                "UPDATE recalls SET state='closed',revision=revision+1 WHERE id=?",
                (recall_id,),
            )
            self.journal.record(
                "recall_closed", recall_id, {"impact_digest": recall["impact_digest"]}
            )
        return self.get(recall_id)

    def certificate(self, certificate_id, material, start, end):
        key(certificate_id)
        key(material)
        start, end = timestamp(start), timestamp(end)
        if end <= start:
            fail("input", "certificate interval must increase")
        with self.journal.transaction() as con:
            old = self.journal.one(
                "SELECT * FROM certificates WHERE id=?", (certificate_id,)
            )
            if old:
                if (old["material"], old["valid_from"], old["valid_to"]) != (
                    material,
                    start,
                    end,
                ):
                    fail("conflict", "certificate definition is immutable")
                return old
            con.execute(
                "INSERT INTO certificates VALUES(?,?,?,?,0)",
                (certificate_id, material, start, end),
            )
            self.journal.record(
                "certificate_registered",
                certificate_id,
                {"material": material, "start": start, "end": end},
            )
        return self.journal.one(
            "SELECT * FROM certificates WHERE id=?", (certificate_id,)
        )

    def revoke_certificate(self, certificate_id, reason):
        if not isinstance(reason, str) or not reason.strip():
            fail("input", "revocation requires explanation")
        with self.journal.transaction() as con:
            old = self.journal.one(
                "SELECT * FROM certificates WHERE id=?", (certificate_id,)
            )
            if old is None:
                fail("missing", "certificate not found")
            con.execute(
                "UPDATE certificates SET revoked=1 WHERE id=?", (certificate_id,)
            )
            roots = self.journal.rows(
                "SELECT id FROM lots WHERE certificate=? ORDER BY id", (certificate_id,)
            )
            affected = sorted(
                {
                    lot
                    for root in roots
                    for lot in [root["id"]] + self.graph.descendants(root["id"])
                }
            )
            if not old["revoked"]:
                self.journal.record(
                    "certificate_revoked",
                    certificate_id,
                    {"reason": reason, "affected": affected},
                )
        return {
            "certificate": certificate_id,
            "affected": affected,
            "changed": not bool(old["revoked"]),
        }
