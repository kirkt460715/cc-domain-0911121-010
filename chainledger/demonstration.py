from .service import SupplyChain


def run():
    with SupplyChain() as app:
        app.consumer.register("supplier")
        app.consumer.register("factory")
        transform = {
            "id": "split",
            "source": "factory",
            "offset": 0,
            "occurred_at": "2026-11-11T01:00:00Z",
            "operations": [
                {
                    "id": "split-op",
                    "kind": "transform",
                    "inputs": [{"lot": "raw", "quantity": "10"}],
                    "outputs": [
                        {"lot": "child", "quantity": "9", "location": "warehouse"}
                    ],
                    "waste": "1",
                    "process": "cutting",
                }
            ],
        }
        blocked = app.consumer.submit(transform)
        app.consumer.submit(
            {
                "id": "receipt",
                "source": "supplier",
                "offset": 0,
                "occurred_at": "2026-11-11T00:00:00Z",
                "operations": [
                    {
                        "id": "receive-op",
                        "kind": "receive",
                        "lot": "raw",
                        "quantity": "10",
                        "material": "alloy",
                        "unit": "kg",
                        "supplier": "synthetic-supplier",
                        "location": "inbox",
                    }
                ],
            }
        )
        app.consumer.submit(
            {
                "id": "shipment-event",
                "source": "factory",
                "offset": 1,
                "occurred_at": "2026-11-11T02:00:00Z",
                "operations": [
                    {
                        "id": "ship-op",
                        "kind": "ship",
                        "lot": "child",
                        "quantity": "4",
                        "location": "warehouse",
                        "customer": "customer-A",
                        "shipment": "shipment-A",
                    }
                ],
            }
        )
        recall = app.recalls.open("recall-A", "raw", "Synthetic trace exercise")
        conserved = app.trace.conservation_report()
        assert blocked["state"] == "blocked"
        assert app.consumer.status("split")["state"] == "applied"
        assert app.graph.stock("child") == 5
        assert recall["impact"]["shipments"][0]["path"] == ["raw", "child"]
        assert conserved["materials"][0]["received"] == "10"
        assert conserved["materials"][0]["waste"] == "1"
        return {
            "initial_state": blocked["state"],
            "resolved_state": app.consumer.status("split")["state"],
            "inventory": app.inventory.by_location(),
            "recall": recall,
            "conservation": conserved,
            "check": app.check(),
        }
