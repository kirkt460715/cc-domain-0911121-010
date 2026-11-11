from .graph.quality import QualityRelease
from .views.packets import ShipmentPackets
from .views.picking import Picking
from .journal import Journal
from .graph.material import MaterialGraph
from .events.consumer import Consumer
from .projections import Projections
from .graph.trace import Trace
from .graph.recalls import Recalls
from .graph.reconciliation import Stocktake
from .views.inventory import Inventory
from .events.exchange import Exchange
from .types import fail, decode, fingerprint


class SupplyChain:
    def __init__(self, path=":memory:"):
        self.journal = Journal(path)
        self.graph = MaterialGraph(self.journal)
        self.consumer = Consumer(self.journal, self.graph)
        self.projections = Projections(self.journal, self.graph)
        self.trace = Trace(self.journal, self.graph)
        self.recalls = Recalls(self.journal, self.graph, self.trace)
        self.stocktakes = Stocktake(self.journal, self.graph)
        self.inventory = Inventory(self.journal, self.graph)
        self.exchange = Exchange(self.journal, self.consumer)
        self.quality = QualityRelease(self.journal, self.graph)
        self.packets = ShipmentPackets(self.journal, self.graph, self.trace)
        self.picking = Picking(self.journal, self.graph, self.consumer)

    def check(self):
        sqlite = self.journal.one("PRAGMA integrity_check")
        if list(sqlite.values()) != ["ok"]:
            fail("integrity", "SQLite integrity check failed")
        audit = self.journal.verify_audit()
        conservation = self.trace.conservation_report()
        topology = self.trace.topology()
        for row in self.journal.rows("SELECT * FROM envelopes"):
            if fingerprint(decode(row["payload"])) != row["digest"]:
                fail("integrity", "envelope digest mismatch", id=row["id"])
            if row["state"] == "applied" and row["applied_order"] is None:
                fail("integrity", "applied event lacks order")
        for source in self.journal.rows("SELECT * FROM sources"):
            invalid = self.journal.rows(
                "SELECT id FROM envelopes WHERE source=? AND offset<? AND state NOT IN ('applied','skipped')",
                (source["id"], source["next_offset"]),
            )
            if invalid:
                fail("integrity", "source acknowledgment crosses unresolved event")
        return {
            "sqlite": "ok",
            "audit": audit,
            "conservation": conservation,
            "topology": topology,
            "pending": len(self.consumer.pending()),
        }

    def close(self):
        self.journal.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
