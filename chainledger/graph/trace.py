from collections import deque
from decimal import Decimal, localcontext
from ..types import fail, text, integer


class Trace:
    def __init__(self, journal, graph):
        self.journal = journal
        self.graph = graph

    def path(self, ancestor, descendant):
        self.graph.lot(ancestor)
        self.graph.lot(descendant)
        queue = deque([(ancestor, [ancestor])])
        visited = {ancestor}
        while queue:
            current, path = queue.popleft()
            if current == descendant:
                return path
            for row in self.journal.rows(
                "SELECT child FROM edges WHERE parent=? ORDER BY child", (current,)
            ):
                if row["child"] not in visited:
                    visited.add(row["child"])
                    queue.append((row["child"], path + [row["child"]]))
        return None

    def subgraph(self, lot, direction="downstream", maximum=10000):
        integer(maximum, "maximum", 1, 100000)
        if direction not in {"downstream", "upstream", "both"}:
            fail("input", "unsupported graph direction")
        self.graph.lot(lot)
        members = {lot}
        if direction in {"upstream", "both"}:
            members.update(self.graph.ancestors(lot))
        if direction in {"downstream", "both"}:
            members.update(self.graph.descendants(lot))
        if len(members) > maximum:
            fail("limit", "trace exceeds maximum nodes", count=len(members))
        edges = [
            row
            for row in self.journal.rows("SELECT * FROM edges ORDER BY parent,child")
            if row["parent"] in members and row["child"] in members
        ]
        return {
            "root": lot,
            "direction": direction,
            "nodes": [self.graph.lot(member) for member in sorted(members)],
            "edges": edges,
        }

    def root_contributions(self, lot):
        self.graph.lot(lot)
        memo = {}
        visiting = set()

        def visit(current):
            if current in memo:
                return memo[current]
            if current in visiting:
                fail("integrity", "cycle in lot genealogy")
            visiting.add(current)
            producing = self.journal.one(
                "SELECT transform_id FROM outputs WHERE lot=?", (current,)
            )
            if producing is None:
                result = {current: Decimal(1)}
            else:
                inputs = self.journal.rows(
                    "SELECT lot,quantity FROM inputs WHERE transform_id=? ORDER BY lot",
                    (producing["transform_id"],),
                )
                total = sum(Decimal(item["quantity"]) for item in inputs)
                result = {}
                with localcontext() as ctx:
                    ctx.prec = 40
                    for item in inputs:
                        ratio = Decimal(item["quantity"]) / total
                        for root, fraction in visit(item["lot"]).items():
                            result[root] = (
                                result.get(root, Decimal(0)) + ratio * fraction
                            )
            visiting.remove(current)
            memo[current] = result
            return result

        roots = visit(lot)
        quantity = Decimal(self.graph.lot(lot)["quantity"])
        return {
            "lot": lot,
            "assumption": "homogeneous mixing at each transformation",
            "contributions": [
                {
                    "root": root,
                    "fraction": text(fraction),
                    "attributed_quantity": text(quantity * fraction),
                }
                for root, fraction in sorted(roots.items())
            ],
        }

    def impact(self, lot):
        affected = [lot] + self.graph.descendants(lot)
        shipments = []
        stock = []
        for member in affected:
            for row in self.journal.rows(
                "SELECT * FROM shipments WHERE lot=? ORDER BY id", (member,)
            ):
                outstanding = Decimal(row["quantity"]) - Decimal(row["returned"])
                if outstanding:
                    shipments.append(
                        {
                            **row,
                            "outstanding": text(outstanding),
                            "path": self.path(lot, member),
                        }
                    )
            for row in self.journal.rows(
                "SELECT * FROM balances WHERE lot=? ORDER BY location", (member,)
            ):
                if Decimal(row["quantity"]):
                    stock.append(row)
        customers = {}
        for shipment in shipments:
            material = self.graph.lot(shipment["lot"])
            group = (shipment["customer"], material["material"], material["unit"])
            customers[group] = customers.get(group, Decimal(0)) + Decimal(
                shipment["outstanding"]
            )
        return {
            "root": lot,
            "lots": affected,
            "stock": stock,
            "shipments": shipments,
            "customers": [
                {
                    "customer": group[0],
                    "material": group[1],
                    "unit": group[2],
                    "outstanding": text(quantity),
                }
                for group, quantity in sorted(customers.items())
            ],
        }

    def conservation_report(self):
        self.graph.conservation()
        groups = {}
        for lot in self.journal.rows("SELECT * FROM lots ORDER BY id"):
            group = (lot["material"], lot["unit"])
            totals = groups.setdefault(
                group,
                {
                    "received": Decimal(0),
                    "stock": Decimal(0),
                    "shipped": Decimal(0),
                    "scrap": Decimal(0),
                    "waste": Decimal(0),
                },
            )
            if lot["supplier"] is not None:
                totals["received"] += Decimal(lot["quantity"])
            totals["stock"] += self.graph.stock(lot["id"])
            totals["shipped"] += sum(
                (
                    Decimal(row["quantity"]) - Decimal(row["returned"])
                    for row in self.journal.rows(
                        "SELECT quantity,returned FROM shipments WHERE lot=?",
                        (lot["id"],),
                    )
                ),
                Decimal(0),
            )
            totals["scrap"] += sum(
                (
                    Decimal(row["quantity"])
                    for row in self.journal.rows(
                        "SELECT quantity FROM scraps WHERE lot=?", (lot["id"],)
                    )
                ),
                Decimal(0),
            )
        for row in self.journal.rows("SELECT material,unit,waste FROM transformations"):
            groups[(row["material"], row["unit"])]["waste"] += Decimal(row["waste"])
        results = []
        for (material, unit), totals in sorted(groups.items()):
            difference = (
                totals["received"]
                - totals["stock"]
                - totals["shipped"]
                - totals["scrap"]
                - totals["waste"]
            )
            if difference:
                fail(
                    "integrity",
                    "global material conservation failed",
                    material=material,
                    unit=unit,
                    difference=text(difference),
                )
            results.append(
                {
                    "material": material,
                    "unit": unit,
                    **{name: text(value) for name, value in totals.items()},
                    "difference": "0",
                }
            )
        return {"conserved": True, "materials": results}

    def topology(self):
        lots = [
            row["id"] for row in self.journal.rows("SELECT id FROM lots ORDER BY id")
        ]
        indegree = dict.fromkeys(lots, 0)
        children = {lot: [] for lot in lots}
        for edge in self.journal.rows(
            "SELECT parent,child FROM edges ORDER BY parent,child"
        ):
            if edge["parent"] not in indegree or edge["child"] not in indegree:
                fail("integrity", "edge references missing lot")
            indegree[edge["child"]] += 1
            children[edge["parent"]].append(edge["child"])
        queue = deque(lot for lot in lots if indegree[lot] == 0)
        ordered = []
        depth = dict.fromkeys(lots, 0)
        while queue:
            lot = queue.popleft()
            ordered.append(lot)
            for child in children[lot]:
                depth[child] = max(depth[child], depth[lot] + 1)
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        if len(ordered) != len(lots):
            fail("integrity", "genealogy contains cycle")
        return {
            "order": ordered,
            "depth": depth,
            "maximum_depth": max(depth.values(), default=0),
        }
