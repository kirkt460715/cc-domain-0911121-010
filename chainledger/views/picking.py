from decimal import Decimal
from ..types import (
    key,
    obj,
    array,
    amount,
    text,
    integer,
    canonical,
    decode,
    fingerprint,
    fail,
    timestamp,
)


class Picking:
    def __init__(self, journal, graph, consumer):
        self.journal = journal
        self.graph = graph
        self.consumer = consumer
        with journal.transaction() as con:
            con.execute("""CREATE TABLE IF NOT EXISTS pick_plans(id TEXT PRIMARY KEY,request TEXT NOT NULL,
                payload TEXT NOT NULL,state TEXT NOT NULL,result TEXT)""")

    def plan(self, plan_id, customer, requirements, at):
        key(plan_id)
        key(customer)
        at = timestamp(at)
        requested = []
        seen = set()
        for item in array(requirements, 1, 100):
            obj(
                item,
                {"material", "unit", "quantity", "locations"},
                {"material", "unit", "quantity", "locations"},
            )
            material, unit = key(item["material"]), key(item["unit"])
            if (material, unit) in seen:
                fail("input", "duplicate pick requirement")
            seen.add((material, unit))
            locations = sorted(
                {key(value) for value in array(item["locations"], 1, 100)}
            )
            requested.append(
                {
                    "material": material,
                    "unit": unit,
                    "quantity": text(amount(item["quantity"])),
                    "locations": locations,
                }
            )
        request = {
            "id": plan_id,
            "customer": customer,
            "requirements": requested,
            "at": at,
        }
        with self.journal.transaction() as con:
            old = self.journal.one("SELECT * FROM pick_plans WHERE id=?", (plan_id,))
            if old:
                if old["request"] != canonical(request):
                    fail("conflict", "pick plan identity differs")
                return self.get(plan_id)
            allocations = []
            shortages = []
            baseline = []
            for item in requested:
                remaining = Decimal(item["quantity"])
                candidates = self.journal.rows(
                    """SELECT l.*,b.location,b.quantity AS available FROM lots l
                    JOIN balances b ON l.id=b.lot WHERE l.material=? AND l.unit=? ORDER BY l.created_at,l.id,b.location""",
                    (item["material"], item["unit"]),
                )
                for candidate in candidates:
                    if (
                        candidate["location"] not in item["locations"]
                        or candidate["created_at"] > at
                    ):
                        continue
                    quantity = Decimal(candidate["available"])
                    if quantity <= 0 or self.graph.blocked(candidate["id"], at):
                        continue
                    taken = min(remaining, quantity)
                    if taken:
                        allocations.append(
                            {
                                "lot": candidate["id"],
                                "location": candidate["location"],
                                "quantity": text(taken),
                                "material": item["material"],
                                "unit": item["unit"],
                            }
                        )
                        baseline.append(
                            {
                                "lot": candidate["id"],
                                "location": candidate["location"],
                                "quantity": text(quantity),
                                "record_digest": fingerprint(
                                    self.graph.lot(candidate["id"])
                                ),
                            }
                        )
                        remaining -= taken
                    if remaining == 0:
                        break
                if remaining:
                    shortages.append(
                        {
                            "material": item["material"],
                            "unit": item["unit"],
                            "quantity": text(remaining),
                        }
                    )
            payload = {
                "id": plan_id,
                "customer": customer,
                "at": at,
                "allocations": allocations,
                "shortages": shortages,
                "baseline": baseline,
                "ready": not shortages,
                "reservation": False,
            }
            con.execute(
                "INSERT INTO pick_plans VALUES(?,?,?,'planned',NULL)",
                (plan_id, canonical(request), canonical(payload)),
            )
            self.journal.record(
                "pick_planned",
                plan_id,
                {"allocations": len(allocations), "shortages": shortages},
            )
        return self.get(plan_id)

    def get(self, plan_id):
        row = self.journal.one("SELECT * FROM pick_plans WHERE id=?", (plan_id,))
        if row is None:
            fail("missing", "pick plan not found")
        return {
            **decode(row["payload"]),
            "state": row["state"],
            "result": decode(row["result"]) if row["result"] else None,
        }

    def dispatch(self, plan_id, event_id, source, offset):
        key(event_id)
        key(source)
        integer(offset, "offset")
        with self.journal.transaction() as con:
            plan = self.get(plan_id)
            if plan["state"] == "dispatched":
                if plan["result"]["event"] != event_id:
                    fail("conflict", "plan already dispatched by a different event")
                return plan
            if plan["state"] != "planned" or not plan["ready"]:
                fail("state", "pick plan is cancelled or has shortages")
            current = self.journal.one(
                "SELECT next_offset FROM sources WHERE id=?", (source,)
            )
            if current is None or current["next_offset"] != offset:
                fail("offset", "dispatch requires current source head offset")
            for baseline in plan["baseline"]:
                if self.graph.balance(baseline["lot"], baseline["location"]) != Decimal(
                    baseline["quantity"]
                ):
                    fail(
                        "conflict",
                        "pick stock changed; create a new plan",
                        lot=baseline["lot"],
                    )
                if (
                    fingerprint(self.graph.lot(baseline["lot"]))
                    != baseline["record_digest"]
                ):
                    fail("conflict", "pick lot definition changed")
                self.graph.ensure_usable(baseline["lot"], plan["at"])
            operations = []
            for index, item in enumerate(plan["allocations"]):
                identifier = event_id + ":" + str(index)
                key(identifier)
                operations.append(
                    {
                        "id": identifier,
                        "kind": "ship",
                        "lot": item["lot"],
                        "location": item["location"],
                        "quantity": item["quantity"],
                        "customer": plan["customer"],
                        "shipment": identifier,
                    }
                )
            result = self.consumer.submit(
                {
                    "id": event_id,
                    "source": source,
                    "offset": offset,
                    "occurred_at": plan["at"],
                    "operations": operations,
                },
                drain=False,
            )
            self.consumer.drain()
            status = self.consumer.status(event_id)
            if status["state"] != "applied":
                fail("state", "pick dispatch could not apply atomically", event=status)
            result = {
                "event": event_id,
                "shipments": [item["shipment"] for item in operations],
            }
            con.execute(
                "UPDATE pick_plans SET state='dispatched',result=? WHERE id=?",
                (canonical(result), plan_id),
            )
            self.journal.record("pick_dispatched", plan_id, result)
        return self.get(plan_id)

    def cancel(self, plan_id, reason):
        if not isinstance(reason, str) or not reason.strip():
            fail("input", "cancellation needs a reason")
        with self.journal.transaction() as con:
            plan = self.get(plan_id)
            if plan["state"] != "planned":
                fail("state", "only a planned pick can be cancelled")
            con.execute(
                "UPDATE pick_plans SET state='cancelled',result=? WHERE id=?",
                (canonical({"reason": reason}), plan_id),
            )
            self.journal.record("pick_cancelled", plan_id, {"reason": reason})
        return self.get(plan_id)

    def queue(self, customer=None):
        plans = []
        for row in self.journal.rows(
            "SELECT id,payload FROM pick_plans WHERE state='planned' ORDER BY id"
        ):
            if customer is None or decode(row["payload"])["customer"] == customer:
                plans.append(self.get(row["id"]))
        return plans

    def freshness(self, plan_id):
        plan = self.get(plan_id)
        changes = []
        for baseline in plan["baseline"]:
            current = self.graph.balance(baseline["lot"], baseline["location"])
            if current != Decimal(baseline["quantity"]):
                changes.append(
                    {
                        "lot": baseline["lot"],
                        "kind": "stock",
                        "before": baseline["quantity"],
                        "current": text(current),
                    }
                )
            lot = self.graph.lot(baseline["lot"])
            if fingerprint(lot) != baseline["record_digest"]:
                changes.append({"lot": baseline["lot"], "kind": "lot_definition"})
            blocks = self.graph.blocked(baseline["lot"], plan["at"])
            if blocks:
                changes.append(
                    {"lot": baseline["lot"], "kind": "quality_hold", "blocks": blocks}
                )
        return {
            "id": plan_id,
            "state": plan["state"],
            "ready": plan["ready"],
            "fresh": not changes,
            "changes": changes,
            "dispatchable": plan["state"] == "planned"
            and plan["ready"]
            and not changes,
        }
