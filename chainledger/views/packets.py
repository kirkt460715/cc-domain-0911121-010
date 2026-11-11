from ..types import key, canonical, decode, fingerprint, fail
from ..events.exchange import write_new, read_file


class ShipmentPackets:
    def __init__(self, journal, graph, trace):
        self.journal = journal
        self.graph = graph
        self.trace = trace
        with journal.transaction() as con:
            con.execute(
                "CREATE TABLE IF NOT EXISTS shipment_packets(id TEXT PRIMARY KEY,payload TEXT NOT NULL,digest TEXT NOT NULL)"
            )

    def create(self, packet_id, shipment_id):
        key(packet_id)
        with self.journal.transaction() as con:
            shipment = self.journal.one(
                "SELECT * FROM shipments WHERE id=?", (shipment_id,)
            )
            if shipment is None:
                fail("missing", "shipment not found")
            graph = self.trace.subgraph(shipment["lot"], "upstream")
            certificates = []
            for cert_id in sorted(
                {node["certificate"] for node in graph["nodes"] if node["certificate"]}
            ):
                certificates.append(
                    self.journal.one(
                        "SELECT * FROM certificates WHERE id=?", (cert_id,)
                    )
                )
            transforms = sorted({edge["transform_id"] for edge in graph["edges"]})
            processes = []
            for transform_id in transforms:
                processes.append(
                    {
                        "transform": self.journal.one(
                            "SELECT * FROM transformations WHERE id=?", (transform_id,)
                        ),
                        "inputs": self.journal.rows(
                            "SELECT lot,quantity FROM inputs WHERE transform_id=? ORDER BY lot",
                            (transform_id,),
                        ),
                        "outputs": self.journal.rows(
                            "SELECT lot,quantity FROM outputs WHERE transform_id=? ORDER BY lot",
                            (transform_id,),
                        ),
                    }
                )
            payload = {
                "schema": 1,
                "id": packet_id,
                "shipment": shipment,
                "graph": graph,
                "certificates": certificates,
                "transformations": processes,
                "contributions": self.trace.root_contributions(shipment["lot"]),
                "current_blocks": self.graph.blocked(shipment["lot"]),
            }
            digest = fingerprint(payload)
            old = self.journal.one(
                "SELECT * FROM shipment_packets WHERE id=?", (packet_id,)
            )
            if old:
                if old["digest"] != digest:
                    fail("conflict", "packet id refers to different evidence")
                return {"id": packet_id, "digest": digest, "reused": True}
            con.execute(
                "INSERT INTO shipment_packets VALUES(?,?,?)",
                (packet_id, canonical(payload), digest),
            )
            self.journal.record(
                "shipment_packet",
                packet_id,
                {"shipment": shipment_id, "digest": digest},
            )
        return {"id": packet_id, "digest": digest, "reused": False}

    def get(self, packet_id):
        row = self.journal.one(
            "SELECT * FROM shipment_packets WHERE id=?", (packet_id,)
        )
        if row is None:
            fail("missing", "shipment packet not found")
        payload = decode(row["payload"])
        if fingerprint(payload) != row["digest"]:
            fail("integrity", "packet digest mismatch")
        return {"payload": payload, "digest": row["digest"]}

    def export(self, packet_id, path):
        packet = self.get(packet_id)
        return {
            **write_new(path, (canonical(packet) + "\n").encode()),
            "digest": packet["digest"],
        }


def verify_packet(path):
    from decimal import Decimal

    packet = decode(read_file(path))
    if not isinstance(packet, dict) or set(packet) != {"payload", "digest"}:
        fail("input", "invalid packet envelope")
    payload = packet["payload"]
    if fingerprint(payload) != packet["digest"]:
        fail("integrity", "packet fingerprint mismatch")
    if payload.get("schema") != 1:
        fail("input", "unsupported packet schema")
    nodes = {node["id"] for node in payload["graph"]["nodes"]}
    if payload["shipment"]["lot"] not in nodes:
        fail("integrity", "shipment lot missing from genealogy")
    for edge in payload["graph"]["edges"]:
        if edge["parent"] not in nodes or edge["child"] not in nodes:
            fail("integrity", "packet graph edge is incomplete")
    for process in payload["transformations"]:
        inputs = sum(Decimal(item["quantity"]) for item in process["inputs"])
        outputs = sum(Decimal(item["quantity"]) for item in process["outputs"])
        if inputs != outputs + Decimal(process["transform"]["waste"]):
            fail("integrity", "packet material conservation failure")
    return {
        "id": payload["id"],
        "shipment": payload["shipment"]["id"],
        "lots": len(nodes),
        "digest": packet["digest"],
    }
