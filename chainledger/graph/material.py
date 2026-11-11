from decimal import Decimal
from ..types import amount, text, fail, canonical


class MaterialGraph:
    def __init__(self, journal):
        self.journal = journal
        self.replaying = False

    def lot(self, lot):
        row = self.journal.one("SELECT * FROM lots WHERE id=?", (lot,))
        if row is None:
            fail("dependency", "lot is not yet known", lot=lot)
        return row

    def balance(self, lot, location):
        row = self.journal.one(
            "SELECT quantity FROM balances WHERE lot=? AND location=?", (lot, location)
        )
        return Decimal(row["quantity"]) if row else Decimal(0)

    def stock(self, lot):
        self.lot(lot)
        return sum(
            (
                Decimal(row["quantity"])
                for row in self.journal.rows(
                    "SELECT quantity FROM balances WHERE lot=?", (lot,)
                )
            ),
            Decimal(0),
        )

    def change(self, lot, location, delta):
        current = self.balance(lot, location)
        updated = current + delta
        if updated < 0:
            fail(
                "insufficient",
                "location has insufficient stock",
                lot=lot,
                location=location,
                available=text(current),
            )
        self.journal.connection.execute(
            "INSERT INTO balances VALUES(?,?,?) ON CONFLICT(lot,location) DO UPDATE SET quantity=excluded.quantity",
            (lot, location, text(updated)),
        )

    def consume(self, lot, quantity):
        if self.stock(lot) < quantity:
            fail(
                "insufficient",
                "lot has insufficient stock",
                lot=lot,
                required=text(quantity),
            )
        remaining = quantity
        for row in self.journal.rows(
            "SELECT location,quantity FROM balances WHERE lot=? ORDER BY location",
            (lot,),
        ):
            taken = min(remaining, Decimal(row["quantity"]))
            if taken:
                self.change(lot, row["location"], -taken)
                remaining -= taken
            if not remaining:
                break

    def ancestors(self, lot):
        self.lot(lot)
        seen = set()
        stack = [lot]
        while stack:
            current = stack.pop()
            for row in self.journal.rows(
                "SELECT parent FROM edges WHERE child=? ORDER BY parent", (current,)
            ):
                if row["parent"] not in seen:
                    seen.add(row["parent"])
                    stack.append(row["parent"])
        return sorted(seen)

    def descendants(self, lot):
        self.lot(lot)
        seen = set()
        stack = [lot]
        while stack:
            current = stack.pop()
            for row in self.journal.rows(
                "SELECT child FROM edges WHERE parent=? ORDER BY child", (current,)
            ):
                if row["child"] not in seen:
                    seen.add(row["child"])
                    stack.append(row["child"])
        return sorted(seen)

    def blocked(self, lot, at=None):
        lineage = [lot] + self.ancestors(lot)
        reasons = []
        for member in lineage:
            holds = self.journal.rows(
                "SELECT id,reason FROM holds WHERE lot=? AND active=1 ORDER BY id",
                (member,),
            )
            reasons.extend({"kind": "hold", "lot": member, **hold} for hold in holds)
            source = self.lot(member)
            if source["certificate"]:
                cert = self.journal.one(
                    "SELECT * FROM certificates WHERE id=?", (source["certificate"],)
                )
                if cert is None or (cert["revoked"] and not self.replaying):
                    reasons.append(
                        {
                            "kind": "certificate",
                            "lot": member,
                            "id": source["certificate"],
                        }
                    )
                elif at is not None and not cert["valid_from"] <= at < cert["valid_to"]:
                    reasons.append(
                        {
                            "kind": "expired_certificate",
                            "lot": member,
                            "id": source["certificate"],
                        }
                    )
        return reasons

    def ensure_usable(self, lot, at):
        reasons = self.blocked(lot, at)
        if reasons:
            fail("hold", "lot or an ancestor is blocked", lot=lot, reasons=reasons)

    def apply(self, envelope):
        for op in envelope.operations:
            if self.journal.one("SELECT id FROM operations WHERE id=?", (op["id"],)):
                fail("conflict", "operation id already applied", operation=op["id"])
            handler = getattr(self, "apply_" + op["kind"])
            handler(op, envelope)
            self.journal.connection.execute(
                "INSERT INTO operations VALUES(?,?,?,?)",
                (op["id"], envelope.id, op["kind"], canonical(op)),
            )

    def apply_receive(self, op, envelope):
        if self.journal.one("SELECT id FROM lots WHERE id=?", (op["lot"],)):
            fail("conflict", "received lot already exists", lot=op["lot"])
        if op["certificate"]:
            cert = self.journal.one(
                "SELECT * FROM certificates WHERE id=?", (op["certificate"],)
            )
            if cert is None:
                fail(
                    "dependency",
                    "certificate not registered",
                    certificate=op["certificate"],
                )
            if cert["material"] != op["material"]:
                fail("material", "certificate material differs")
            if (cert["revoked"] and not self.replaying) or not cert[
                "valid_from"
            ] <= envelope.occurred_at < cert["valid_to"]:
                fail("certificate", "receipt outside valid certificate interval")
        self.journal.connection.execute(
            "INSERT INTO lots VALUES(?,?,?,?,?,?,?,?)",
            (
                op["lot"],
                op["material"],
                op["unit"],
                op["quantity"],
                op["supplier"],
                op["certificate"],
                op["id"],
                envelope.occurred_at,
            ),
        )
        self.change(op["lot"], op["location"], amount(op["quantity"]))

    def apply_transform(self, op, envelope):
        lots = [self.lot(item["lot"]) for item in op["inputs"]]
        materials = {lot["material"] for lot in lots}
        units = {lot["unit"] for lot in lots}
        if len(materials) != 1 or len(units) != 1:
            fail("material", "transformation requires matching material and unit")
        total_in = sum(amount(item["quantity"]) for item in op["inputs"])
        total_out = sum(amount(item["quantity"]) for item in op["outputs"])
        waste = amount(op["waste"], allow_zero=True)
        if total_in != total_out + waste:
            fail(
                "conservation",
                "transformation inputs must equal outputs plus declared waste",
                inputs=text(total_in),
                outputs=text(total_out),
                waste=text(waste),
            )
        for lot in lots:
            if lot["created_at"] > envelope.occurred_at:
                fail("time", "transformation predates input receipt")
            self.ensure_usable(lot["id"], envelope.occurred_at)
        for item in op["outputs"]:
            if self.journal.one("SELECT id FROM lots WHERE id=?", (item["lot"],)):
                fail("conflict", "output lot already exists", lot=item["lot"])
        material = next(iter(materials))
        unit = next(iter(units))
        self.journal.connection.execute(
            "INSERT INTO transformations VALUES(?,?,?,?,?)",
            (op["id"], op["process"], op["waste"], material, unit),
        )
        for item in op["inputs"]:
            self.consume(item["lot"], amount(item["quantity"]))
            self.journal.connection.execute(
                "INSERT INTO inputs VALUES(?,?,?)",
                (op["id"], item["lot"], item["quantity"]),
            )
        for item in op["outputs"]:
            self.journal.connection.execute(
                "INSERT INTO lots VALUES(?,?,?,?,NULL,NULL,?,?)",
                (
                    item["lot"],
                    material,
                    unit,
                    item["quantity"],
                    op["id"],
                    envelope.occurred_at,
                ),
            )
            self.change(item["lot"], item["location"], amount(item["quantity"]))
            self.journal.connection.execute(
                "INSERT INTO outputs VALUES(?,?,?)",
                (op["id"], item["lot"], item["quantity"]),
            )
            for parent in op["inputs"]:
                self.journal.connection.execute(
                    "INSERT INTO edges VALUES(?,?,?)",
                    (parent["lot"], item["lot"], op["id"]),
                )

    def apply_move(self, op, envelope):
        lot = self.lot(op["lot"])
        if lot["created_at"] > envelope.occurred_at:
            fail("time", "movement predates receipt")
        quantity = amount(op["quantity"])
        self.change(op["lot"], op["from"], -quantity)
        self.change(op["lot"], op["to"], quantity)

    def apply_ship(self, op, envelope):
        self.lot(op["lot"])
        self.ensure_usable(op["lot"], envelope.occurred_at)
        if self.journal.one("SELECT id FROM shipments WHERE id=?", (op["shipment"],)):
            fail("conflict", "shipment already exists")
        self.change(op["lot"], op["location"], -amount(op["quantity"]))
        self.journal.connection.execute(
            "INSERT INTO shipments VALUES(?,?,?,?,?,?)",
            (op["shipment"], op["lot"], op["customer"], op["quantity"], "0", op["id"]),
        )

    def apply_return(self, op, envelope):
        shipment = self.journal.one(
            "SELECT * FROM shipments WHERE id=?", (op["shipment"],)
        )
        if shipment is None:
            fail("dependency", "shipment not known", shipment=op["shipment"])
        returned = Decimal(shipment["returned"]) + amount(op["quantity"])
        if returned > Decimal(shipment["quantity"]):
            fail("conservation", "returned quantity exceeds shipment")
        self.change(shipment["lot"], op["location"], amount(op["quantity"]))
        self.journal.connection.execute(
            "UPDATE shipments SET returned=? WHERE id=?",
            (text(returned), op["shipment"]),
        )

    def apply_hold(self, op, envelope):
        self.lot(op["lot"])
        self.journal.connection.execute(
            "INSERT INTO holds VALUES(?,?,?,1)", (op["id"], op["lot"], op["reason"])
        )

    def apply_release(self, op, envelope):
        hold = self.journal.one("SELECT * FROM holds WHERE id=?", (op["hold"],))
        if hold is None:
            fail("dependency", "hold not yet known")
        if hold["lot"] != op["lot"] or not hold["active"]:
            fail("state", "hold does not match lot or is already released")
        self.journal.connection.execute(
            "UPDATE holds SET active=0 WHERE id=?", (op["hold"],)
        )

    def apply_scrap(self, op, envelope):
        self.lot(op["lot"])
        self.change(op["lot"], op["location"], -amount(op["quantity"]))
        self.journal.connection.execute(
            "INSERT INTO scraps VALUES(?,?,?,?)",
            (op["id"], op["lot"], op["quantity"], op["reason"]),
        )

    def conservation(self):
        issues = []
        for lot in self.journal.rows("SELECT * FROM lots ORDER BY id"):
            consumed = sum(
                (
                    Decimal(row["quantity"])
                    for row in self.journal.rows(
                        "SELECT quantity FROM inputs WHERE lot=?", (lot["id"],)
                    )
                ),
                Decimal(0),
            )
            shipped = sum(
                (
                    Decimal(row["quantity"]) - Decimal(row["returned"])
                    for row in self.journal.rows(
                        "SELECT quantity,returned FROM shipments WHERE lot=?",
                        (lot["id"],),
                    )
                ),
                Decimal(0),
            )
            scrapped = sum(
                (
                    Decimal(row["quantity"])
                    for row in self.journal.rows(
                        "SELECT quantity FROM scraps WHERE lot=?", (lot["id"],)
                    )
                ),
                Decimal(0),
            )
            stock = self.stock(lot["id"])
            difference = (
                Decimal(lot["quantity"]) - consumed - shipped - scrapped - stock
            )
            if difference:
                issues.append({"lot": lot["id"], "difference": text(difference)})
        if issues:
            fail("integrity", "lot conservation failed", issues=issues)
        return {
            "lots": self.journal.one("SELECT COUNT(*) AS n FROM lots")["n"],
            "conserved": True,
        }
