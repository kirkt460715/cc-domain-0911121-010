from ..types import obj, array, key, amount, text, integer, timestamp, fingerprint, fail


class Envelope:
    def __init__(self, data):
        obj(
            data,
            {"id", "source", "offset", "occurred_at", "operations"},
            {"id", "source", "offset", "occurred_at", "operations"},
        )
        self.id = key(data["id"])
        self.source = key(data["source"])
        self.offset = integer(data["offset"], "offset")
        self.occurred_at = timestamp(data["occurred_at"])
        self.operations = [
            operation(item) for item in array(data["operations"], 1, 1000)
        ]
        ids = [item["id"] for item in self.operations]
        if len(ids) != len(set(ids)):
            fail("input", "operation identifiers must be unique within transaction")

    def as_dict(self):
        return {
            "id": self.id,
            "source": self.source,
            "offset": self.offset,
            "occurred_at": self.occurred_at,
            "operations": self.operations,
        }

    @property
    def digest(self):
        return fingerprint(self.as_dict())


def portion(data, output=False):
    fields = {"lot", "quantity", "location"} if output else {"lot", "quantity"}
    obj(data, fields, fields)
    result = {"lot": key(data["lot"]), "quantity": text(amount(data["quantity"]))}
    if output:
        result["location"] = key(data["location"])
    return result


def operation(data):
    obj(data, required={"id", "kind"})
    ident = key(data["id"])
    kind = data["kind"]
    if kind == "receive":
        obj(
            data,
            {
                "id",
                "kind",
                "lot",
                "material",
                "unit",
                "quantity",
                "location",
                "supplier",
                "certificate",
            },
            {
                "id",
                "kind",
                "lot",
                "material",
                "unit",
                "quantity",
                "location",
                "supplier",
            },
        )
        result = {
            "id": ident,
            "kind": kind,
            "lot": key(data["lot"]),
            "material": key(data["material"]),
            "unit": key(data["unit"]),
            "quantity": text(amount(data["quantity"])),
            "location": key(data["location"]),
            "supplier": key(data["supplier"]),
        }
        certificate = data.get("certificate")
        result["certificate"] = key(certificate) if certificate is not None else None
        return result
    if kind == "transform":
        obj(
            data,
            {"id", "kind", "inputs", "outputs", "waste", "process"},
            {"id", "kind", "inputs", "outputs", "waste", "process"},
        )
        inputs = [portion(item) for item in array(data["inputs"], 1, 100)]
        outputs = [portion(item, True) for item in array(data["outputs"], 1, 100)]
        if len({item["lot"] for item in inputs}) != len(inputs) or len(
            {item["lot"] for item in outputs}
        ) != len(outputs):
            fail("input", "transform lot references must be unique")
        if {item["lot"] for item in inputs} & {item["lot"] for item in outputs}:
            fail("input", "transform cannot reuse an input lot as output")
        return {
            "id": ident,
            "kind": kind,
            "inputs": inputs,
            "outputs": outputs,
            "waste": text(amount(data["waste"], allow_zero=True)),
            "process": key(data["process"]),
        }
    if kind == "move":
        obj(
            data,
            {"id", "kind", "lot", "from", "to", "quantity"},
            {"id", "kind", "lot", "from", "to", "quantity"},
        )
        if data["from"] == data["to"]:
            fail("input", "movement locations must differ")
        return {
            "id": ident,
            "kind": kind,
            "lot": key(data["lot"]),
            "from": key(data["from"]),
            "to": key(data["to"]),
            "quantity": text(amount(data["quantity"])),
        }
    if kind == "ship":
        obj(
            data,
            {"id", "kind", "lot", "location", "quantity", "customer", "shipment"},
            {"id", "kind", "lot", "location", "quantity", "customer", "shipment"},
        )
        return {
            "id": ident,
            "kind": kind,
            "lot": key(data["lot"]),
            "location": key(data["location"]),
            "quantity": text(amount(data["quantity"])),
            "customer": key(data["customer"]),
            "shipment": key(data["shipment"]),
        }
    if kind == "return":
        obj(
            data,
            {"id", "kind", "shipment", "quantity", "location"},
            {"id", "kind", "shipment", "quantity", "location"},
        )
        return {
            "id": ident,
            "kind": kind,
            "shipment": key(data["shipment"]),
            "quantity": text(amount(data["quantity"])),
            "location": key(data["location"]),
        }
    if kind == "hold":
        obj(data, {"id", "kind", "lot", "reason"}, {"id", "kind", "lot", "reason"})
        reason = data["reason"]
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 512:
            fail("input", "hold requires bounded reason")
        return {
            "id": ident,
            "kind": kind,
            "lot": key(data["lot"]),
            "reason": reason.strip(),
        }
    if kind == "release":
        obj(data, {"id", "kind", "lot", "hold"}, {"id", "kind", "lot", "hold"})
        return {
            "id": ident,
            "kind": kind,
            "lot": key(data["lot"]),
            "hold": key(data["hold"]),
        }
    if kind == "scrap":
        obj(
            data,
            {"id", "kind", "lot", "location", "quantity", "reason"},
            {"id", "kind", "lot", "location", "quantity", "reason"},
        )
        return {
            "id": ident,
            "kind": kind,
            "lot": key(data["lot"]),
            "location": key(data["location"]),
            "quantity": text(amount(data["quantity"])),
            "reason": key(data["reason"]),
        }
    fail("input", "unknown supply event kind", kind=kind)


def dependencies(envelope):
    needed = set()
    created = set()
    for op in envelope.operations:
        if op["kind"] == "receive":
            created.add(op["lot"])
        elif op["kind"] == "transform":
            needed.update(
                item["lot"] for item in op["inputs"] if item["lot"] not in created
            )
            created.update(item["lot"] for item in op["outputs"])
        elif "lot" in op and op["lot"] not in created:
            needed.add(op["lot"])
    return sorted(needed)
