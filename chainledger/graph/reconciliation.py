from decimal import Decimal
from ..types import (
    key,
    obj,
    amount,
    text,
    canonical,
    decode,
    fail,
)


class Stocktake:
    def __init__(self, journal, graph):
        self.journal = journal
        self.graph = graph
        with journal.transaction() as con:
            con.execute("""CREATE TABLE IF NOT EXISTS stocktakes(id TEXT PRIMARY KEY,location TEXT NOT NULL,
                baseline TEXT NOT NULL,counts TEXT,state TEXT NOT NULL,revision INTEGER NOT NULL)""")

    def open(self, stocktake_id, location):
        key(stocktake_id)
        key(location)
        with self.journal.transaction() as con:
            baseline = {
                row["lot"]: row["quantity"]
                for row in self.journal.rows(
                    "SELECT lot,quantity FROM balances WHERE location=? ORDER BY lot",
                    (location,),
                )
                if Decimal(row["quantity"])
            }
            old = self.journal.one(
                "SELECT * FROM stocktakes WHERE id=?", (stocktake_id,)
            )
            if old:
                if old["location"] != location:
                    fail("conflict", "stocktake location differs")
                return self.get(stocktake_id)
            con.execute(
                "INSERT INTO stocktakes VALUES(?,?,?,NULL,'counting',1)",
                (stocktake_id, location, canonical(baseline)),
            )
            self.journal.record(
                "stocktake_opened",
                stocktake_id,
                {"location": location, "baseline": baseline},
            )
        return self.get(stocktake_id)

    def get(self, stocktake_id):
        row = self.journal.one("SELECT * FROM stocktakes WHERE id=?", (stocktake_id,))
        if row is None:
            fail("missing", "stocktake not found")
        return {
            **row,
            "baseline": decode(row["baseline"]),
            "counts": decode(row["counts"]) if row["counts"] else None,
        }

    def count(self, stocktake_id, counts, expected_revision):
        obj(counts)
        values = {
            key(lot): text(amount(quantity, allow_zero=True))
            for lot, quantity in counts.items()
        }
        with self.journal.transaction() as con:
            stocktake = self.get(stocktake_id)
            if (
                stocktake["state"] != "counting"
                or stocktake["revision"] != expected_revision
            ):
                fail("conflict", "stocktake state or revision changed")
            if set(stocktake["baseline"]) - set(values):
                fail(
                    "input",
                    "count must include every baseline lot, including zero counts",
                )
            for lot in values:
                self.graph.lot(lot)
            con.execute(
                "UPDATE stocktakes SET counts=?,revision=revision+1 WHERE id=?",
                (canonical(values), stocktake_id),
            )
            self.journal.record("stocktake_counted", stocktake_id, {"counts": values})
        return self.differences(stocktake_id)

    def differences(self, stocktake_id):
        stocktake = self.get(stocktake_id)
        if stocktake["counts"] is None:
            fail("state", "stocktake has no count")
        result = []
        for lot in sorted(stocktake["counts"].keys() | stocktake["baseline"].keys()):
            baseline = Decimal(stocktake["baseline"].get(lot, "0"))
            counted = Decimal(stocktake["counts"].get(lot, "0"))
            current = self.graph.balance(lot, stocktake["location"])
            result.append(
                {
                    "lot": lot,
                    "baseline": text(baseline),
                    "counted": text(counted),
                    "current": text(current),
                    "difference": text(counted - baseline),
                    "movement_since_open": text(current - baseline),
                }
            )
        return {
            "id": stocktake_id,
            "location": stocktake["location"],
            "revision": stocktake["revision"],
            "items": result,
            "balanced": all(Decimal(row["difference"]) == 0 for row in result),
            "stable": all(Decimal(row["movement_since_open"]) == 0 for row in result),
        }

    def close(self, stocktake_id, expected_revision):
        with self.journal.transaction() as con:
            stocktake = self.get(stocktake_id)
            if (
                stocktake["state"] != "counting"
                or stocktake["revision"] != expected_revision
            ):
                fail("conflict", "stocktake state or revision changed")
            difference = self.differences(stocktake_id)
            if not difference["balanced"] or not difference["stable"]:
                fail(
                    "state",
                    "stocktake needs reconciled counts and unchanged inventory",
                    differences=difference,
                )
            con.execute(
                "UPDATE stocktakes SET state='closed',revision=revision+1 WHERE id=?",
                (stocktake_id,),
            )
            self.journal.record(
                "stocktake_closed", stocktake_id, {"revision": expected_revision + 1}
            )
        return self.get(stocktake_id)

    def rebase(self, stocktake_id, expected_revision):
        with self.journal.transaction() as con:
            stocktake = self.get(stocktake_id)
            if (
                stocktake["state"] != "counting"
                or stocktake["revision"] != expected_revision
            ):
                fail("conflict", "stocktake state or revision changed")
            baseline = {
                row["lot"]: row["quantity"]
                for row in self.journal.rows(
                    "SELECT lot,quantity FROM balances WHERE location=? ORDER BY lot",
                    (stocktake["location"],),
                )
                if Decimal(row["quantity"])
            }
            con.execute(
                "UPDATE stocktakes SET baseline=?,counts=NULL,revision=revision+1 WHERE id=?",
                (canonical(baseline), stocktake_id),
            )
            self.journal.record(
                "stocktake_rebased", stocktake_id, {"baseline": baseline}
            )
        return self.get(stocktake_id)
