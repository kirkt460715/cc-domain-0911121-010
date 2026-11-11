from decimal import Decimal
from ..types import text, fail, integer, key, decode


class Inventory:
    def __init__(self, journal, graph):
        self.journal = journal
        self.graph = graph

    def lot(self, lot):
        data = self.graph.lot(lot)
        balances = self.journal.rows(
            "SELECT location,quantity FROM balances WHERE lot=? ORDER BY location",
            (lot,),
        )
        return {
            **data,
            "balances": balances,
            "available": text(self.graph.stock(lot)),
            "holds": self.graph.blocked(lot),
            "ancestors": self.graph.ancestors(lot),
            "descendants": self.graph.descendants(lot),
        }

    def list(
        self, material=None, location=None, include_empty=False, after=None, limit=100
    ):
        integer(limit, "limit", 1, 10000)
        if not isinstance(include_empty, bool):
            fail("input", "include_empty must be boolean")
        result = []
        for row in self.journal.rows("SELECT * FROM lots ORDER BY id"):
            if material is not None and row["material"] != material:
                continue
            if after is not None and row["id"] <= after:
                continue
            quantity = (
                self.graph.balance(row["id"], location)
                if location is not None
                else self.graph.stock(row["id"])
            )
            if not include_empty and quantity == 0:
                continue
            result.append(
                {
                    **row,
                    "available": text(quantity),
                    "blocked": bool(self.graph.blocked(row["id"])),
                }
            )
            if len(result) > limit:
                break
        more = len(result) > limit
        result = result[:limit]
        return {"items": result, "next": result[-1]["id"] if more else None}

    def by_location(self):
        groups = {}
        for row in self.journal.rows("SELECT * FROM balances ORDER BY lot,location"):
            lot = self.graph.lot(row["lot"])
            group = (row["location"], lot["material"], lot["unit"])
            groups[group] = groups.get(group, Decimal(0)) + Decimal(row["quantity"])
        return [
            {
                "location": group[0],
                "material": group[1],
                "unit": group[2],
                "quantity": text(quantity),
            }
            for group, quantity in sorted(groups.items())
            if quantity
        ]

    def supplier_exposure(self, supplier):
        key(supplier)
        roots = [
            row["id"]
            for row in self.journal.rows(
                "SELECT id FROM lots WHERE supplier=? ORDER BY id", (supplier,)
            )
        ]
        descendants = sorted(
            {item for root in roots for item in [root] + self.graph.descendants(root)}
        )
        grouped = {}
        for lot in descendants:
            info = self.graph.lot(lot)
            group = (info["material"], info["unit"])
            grouped[group] = grouped.get(group, Decimal(0)) + self.graph.stock(lot)
        return {
            "supplier": supplier,
            "roots": roots,
            "affected_lots": descendants,
            "current_stock": [
                {"material": group[0], "unit": group[1], "quantity": text(quantity)}
                for group, quantity in sorted(grouped.items())
            ],
        }

    def shipments(self, customer=None, open_only=False):
        result = []
        for row in self.journal.rows("SELECT * FROM shipments ORDER BY id"):
            if customer is not None and row["customer"] != customer:
                continue
            outstanding = Decimal(row["quantity"]) - Decimal(row["returned"])
            if open_only and not outstanding:
                continue
            result.append(
                {
                    **row,
                    "outstanding": text(outstanding),
                    "blocked_ancestry": bool(self.graph.blocked(row["lot"])),
                }
            )
        return result

    def process_yields(self):
        grouped = {}
        for row in self.journal.rows("SELECT * FROM transformations ORDER BY id"):
            incoming = sum(
                Decimal(item["quantity"])
                for item in self.journal.rows(
                    "SELECT quantity FROM inputs WHERE transform_id=?", (row["id"],)
                )
            )
            outgoing = sum(
                Decimal(item["quantity"])
                for item in self.journal.rows(
                    "SELECT quantity FROM outputs WHERE transform_id=?", (row["id"],)
                )
            )
            group = (row["process"], row["material"], row["unit"])
            values = grouped.setdefault(
                group,
                {
                    "input": Decimal(0),
                    "output": Decimal(0),
                    "waste": Decimal(0),
                    "batches": 0,
                },
            )
            values["input"] += incoming
            values["output"] += outgoing
            values["waste"] += Decimal(row["waste"])
            values["batches"] += 1
        return [
            {
                "process": group[0],
                "material": group[1],
                "unit": group[2],
                "input": text(value["input"]),
                "output": text(value["output"]),
                "waste": text(value["waste"]),
                "batches": value["batches"],
                "yield": text(value["output"] / value["input"]),
            }
            for group, value in sorted(grouped.items())
        ]

    def operations(self, lot):
        self.graph.lot(lot)
        related = []
        for row in self.journal.rows("SELECT * FROM operations ORDER BY rowid"):
            payload = decode(row["payload"])
            matches = payload.get("lot") == lot or any(
                item["lot"] == lot
                for item in payload.get("inputs", []) + payload.get("outputs", [])
            )
            if payload["kind"] == "return":
                shipment = self.journal.one(
                    "SELECT lot FROM shipments WHERE id=?", (payload["shipment"],)
                )
                matches = shipment and shipment["lot"] == lot
            if matches:
                related.append(
                    {
                        "id": row["id"],
                        "envelope": row["envelope"],
                        "kind": row["kind"],
                        "operation": payload,
                    }
                )
        return related
