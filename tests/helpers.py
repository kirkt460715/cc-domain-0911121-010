from chainledger.service import SupplyChain

T0 = "2026-11-11T00:00:00Z"
T1 = "2026-11-11T01:00:00Z"
T2 = "2026-11-11T02:00:00Z"


def envelope(ident="e0", source="s", offset=0, ops=None, at=T0):
    return {
        "id": ident,
        "source": source,
        "offset": offset,
        "occurred_at": at,
        "operations": ops or [receive()],
    }


def receive(lot="raw", quantity="10", ident="receive", **changes):
    data = {
        "id": ident,
        "kind": "receive",
        "lot": lot,
        "quantity": quantity,
        "material": "alloy",
        "unit": "kg",
        "supplier": "supplier",
        "location": "A",
    }
    data.update(changes)
    return data


def transform(ident="transform", inputs=None, outputs=None, waste="1"):
    return {
        "id": ident,
        "kind": "transform",
        "inputs": inputs or [{"lot": "raw", "quantity": "10"}],
        "outputs": outputs or [{"lot": "child", "quantity": "9", "location": "B"}],
        "waste": waste,
        "process": "cut",
    }


def shipment(
    ident="ship",
    lot="child",
    quantity="4",
    shipment_id="sh",
    customer="customer",
    location="B",
):
    return {
        "id": ident,
        "kind": "ship",
        "lot": lot,
        "quantity": quantity,
        "shipment": shipment_id,
        "customer": customer,
        "location": location,
    }


def app(path=":memory:"):
    result = SupplyChain(path)
    result.consumer.register("s")
    result.consumer.register("f")
    return result


def built(application):
    application.consumer.submit(envelope())
    application.consumer.submit(envelope("e1", "f", 0, [transform()], T1))
    application.consumer.submit(envelope("e2", "f", 1, [shipment()], T2))
    return application
