"""制造交换 XML 采集适配器。

把工厂追溯交换文件（urn:manufacturing:1 / urn:manufacturing:2）整体转换为
现有事件信封（events.protocol.Envelope），再交给原 Consumer 消费链处理。

设计约束：
- 解析器基于 expat，禁止 DOCTYPE、任何实体声明、外部实体、处理指令与未声明
  命名空间，不访问网络；未知元素或属性不忽略，整文件硬拒收。
- 一个事务转换为一个信封、一个业务操作，成员（投入/产出/损耗）任一无法转换
  时整个事务都不提交；损耗数量原样传递，绝不补平。
- 对照表按 id + revision 固定解释；事务行记录转换时所用映射版本，已接受事务
  不会被后续新映射改变。
- 批次/前序引用是否可应用由原 Consumer 的 dependency 阻塞与 drain 重试负责，
  适配器只负责可查询、可重试的采集状态。
"""

import hashlib
import re
import xml.parsers.expat as expat
from datetime import datetime, timezone
from decimal import localcontext, Decimal

from ..types import (
    ChainError,
    amount,
    canonical,
    decode,
    fail,
    fingerprint,
    integer,
    key,
    obj,
    text,
    timestamp,
)
from .exchange import read_file

NAMESPACES = {
    "urn:manufacturing:1": 1,
    "urn:manufacturing:2": 2,
}
NAMESPACE_BY_VERSION = {version: uri for uri, version in NAMESPACES.items()}
MAX_BYTES = 32_000_000
MAX_NODES = 200_000
MAX_DEPTH = 100
MAX_MEMBERS = 100
DECIMAL_TEXT = re.compile(r"[0-9]+(?:\.[0-9]+)?\Z")


def composite_key(*parts):
    value = ":".join(parts)
    if len(value) > 96:
        fail("input", "composed identifier exceeds supported length")
    return key(value)


class Node:
    """解析得到的元素树节点，保留起始行号作为 XML 位置证据。"""

    def __init__(self, uri, local, line):
        self.uri = uri
        self.local = local
        self.line = line
        self.attrs = {}
        self.children = []
        self.text = ""

    def to_dict(self):
        return {
            "ns": NAMESPACES[self.uri],
            "tag": self.local,
            "line": self.line,
            "attrs": dict(self.attrs),
            "text": self.text,
            "children": [child.to_dict() for child in self.children],
        }


class _XmlReject(ChainError):
    def __init__(self, message, **details):
        super().__init__("xml", message, **details)


def parse_xml(content):
    """安全解析制造交换 XML；任何危险或未声明结构直接拒收。"""
    if not isinstance(content, str) or len(content.encode("utf-8")) > MAX_BYTES:
        fail("input", "manufacturing exchange exceeds byte limit")
    parser = expat.ParserCreate(namespace_separator="\x1f")
    stack = []
    counts = {"nodes": 0}

    def reject(message, **details):
        raise _XmlReject(message, line=parser.CurrentLineNumber, **details)

    def on_start(name, attrs):
        counts["nodes"] += 1
        if counts["nodes"] > MAX_NODES:
            reject("exchange node limit exceeded")
        if "\x1f" not in name:
            reject("element without declared namespace", element=name)
        uri, local = name.split("\x1f", 1)
        if uri not in NAMESPACES:
            reject("undeclared namespace", namespace=uri)
        if len(stack) >= MAX_DEPTH:
            reject("element depth limit exceeded")
        node = Node(uri, local, parser.CurrentLineNumber)
        for qualified, value in attrs.items():
            if "\x1f" in qualified:
                attr_uri, attr_local = qualified.split("\x1f", 1)
                if attr_uri not in NAMESPACES:
                    reject("undeclared attribute namespace", namespace=attr_uri)
            else:
                attr_local = qualified
            node.attrs[attr_local] = value
        if stack:
            stack[-1].children.append(node)
        elif root_ref[0] is not None:
            reject("document must have a single root")
        else:
            root_ref[0] = node
        stack.append(node)

    def on_end(name):
        stack.pop()

    def on_char(data):
        if not stack:
            return
        node = stack[-1]
        node.text += data
        if node.local != "Quantity" and data.strip():
            reject("unexpected text content", element=node.local)

    def on_namespace(prefix, uri):
        if uri not in NAMESPACES:
            reject("undeclared namespace declaration", namespace=uri)

    def on_doctype(name, sysid, pubid, internal):
        reject("doctype declarations are not allowed", doctype=name)

    def on_entity(name, is_param, value, base, sysid, pubid, notation):
        reject("entity declarations are not allowed", entity=name)

    def on_external(context, base, sysid, pubid):
        reject("external entities are not allowed", system_id=sysid)

    def on_notation(name, base, sysid, pubid):
        reject("notation declarations are not allowed", notation=name)

    def on_pi(target, data):
        reject("processing instructions are not allowed", target=target)

    root_ref = [None]
    parser.StartElementHandler = on_start
    parser.EndElementHandler = on_end
    parser.CharacterDataHandler = on_char
    parser.StartNamespaceDeclHandler = on_namespace
    parser.StartDoctypeDeclHandler = on_doctype
    parser.EntityDeclHandler = on_entity
    parser.ExternalEntityRefHandler = on_external
    parser.NotationDeclHandler = on_notation
    parser.ProcessingInstructionHandler = on_pi
    try:
        parser.Parse(content, True)
    except expat.ExpatError as exc:
        raise _XmlReject(
            "malformed manufacturing exchange xml",
            line=exc.lineno,
            reason=expat.ErrorString(exc.code),
        )
    if root_ref[0] is None:
        raise _XmlReject("empty exchange document")
    return root_ref[0]


def _require_attrs(node, required, optional=()):
    present = set(node.attrs)
    missing = set(required) - present
    if missing:
        fail(
            "xml",
            "missing required attributes",
            element=node.local,
            fields=sorted(missing),
            line=node.line,
        )
    unknown = present - set(required) - set(optional)
    if unknown:
        fail(
            "xml",
            "unknown attributes are not allowed",
            element=node.local,
            fields=sorted(unknown),
            line=node.line,
        )


def _children(node, expected_ns):
    for child in node.children:
        if child.uri != expected_ns:
            fail(
                "xml",
                "element namespace must match exchange version",
                element=child.local,
                line=child.line,
            )
        if child.local != "Quantity" and child.text.strip():
            fail("xml", "unexpected text content", element=child.local, line=child.line)
    return node.children


def _bounded_integer(value, field, line):
    if not isinstance(value, str) or not value.isdigit():
        fail("xml", f"{field} must be a non-negative integer", line=line, value=value)
    return integer(int(value), field)


def _quantity(node, allow_zero=False):
    if node.local != "Quantity" or len(node.children):
        fail("xml", "expected a Quantity leaf", line=node.line)
    _require_attrs(node, ("unit",))
    raw = node.text.strip()
    if not DECIMAL_TEXT.fullmatch(raw):
        fail("xml", "quantity must be plain exact decimal", line=node.line, value=raw)
    return node.attrs["unit"], amount(Decimal(raw), allow_zero=allow_zero)


def validate_exchange(root):
    """按声明版本显式分派并做严格结构校验，返回事务节点列表。"""
    if root.local != "Exchange":
        fail("xml", "root must be Exchange", element=root.local, line=root.line)
    version = NAMESPACES[root.uri]
    _require_attrs(root, ("source", "mapping"))
    source = key(root.attrs["source"])
    mapping = key(root.attrs["mapping"])
    transactions = []
    for txn in _children(root, root.uri):
        if txn.local != "Transaction":
            fail(
                "xml",
                "Exchange may only contain Transaction elements",
                element=txn.local,
                line=txn.line,
            )
        _require_attrs(txn, ("id", "sequence", "at"))
        external_id = key(txn.attrs["id"])
        sequence = _bounded_integer(txn.attrs["sequence"], "sequence", txn.line)
        at = timestamp(txn.attrs["at"])
        business = _children(txn, root.uri)
        if len(business) != 1 or business[0].local not in {"Receive", "Transform"}:
            fail(
                "xml",
                "Transaction must contain exactly one Receive or Transform",
                line=txn.line,
            )
        body = business[0]
        if body.local == "Receive":
            _require_attrs(
                body,
                ("id", "lot", "material", "location", "supplier"),
                ("certificate",),
            )
            leaves = _children(body, root.uri)
            if len(leaves) != 1 or leaves[0].local != "Quantity":
                fail("xml", "Receive requires exactly one Quantity", line=body.line)
            _quantity(leaves[0])
        else:
            _require_attrs(body, ("id", "process"))
            counts = {}
            for member in _children(body, root.uri):
                counts[member.local] = counts.get(member.local, 0) + 1
                if member.local == "Input":
                    _require_attrs(member, ("lot",))
                elif member.local == "Output":
                    _require_attrs(member, ("lot", "location"))
                elif member.local == "Waste":
                    _require_attrs(member, ())
                else:
                    fail(
                        "xml",
                        "unknown Transform member",
                        element=member.local,
                        line=member.line,
                    )
                leaves = [child for child in member.children if child.local == "Quantity"]
                if len(leaves) != 1 or len(member.children) != 1:
                    fail(
                        "xml",
                        "Transform member requires exactly one Quantity",
                        element=member.local,
                        line=member.line,
                    )
                _quantity(leaves[0], allow_zero=member.local == "Waste")
            if not (1 <= counts.get("Input", 0) <= MAX_MEMBERS):
                fail("xml", "Transform requires between 1 and 100 inputs", line=body.line)
            if not (1 <= counts.get("Output", 0) <= MAX_MEMBERS):
                fail("xml", "Transform requires between 1 and 100 outputs", line=body.line)
            if counts.get("Waste") != 1:
                fail("xml", "Transform requires exactly one Waste", line=body.line)
        transactions.append(
            {
                "external_id": external_id,
                "sequence": sequence,
                "at": at,
                "body": body,
                "node": txn,
            }
        )
    if not transactions:
        fail("xml", "Exchange must contain at least one transaction")
    return source, mapping, version, transactions


def validate_mapping(data):
    obj(data, ("id", "revision", "materials", "locations", "units"),
        ("id", "revision", "materials", "locations", "units"))
    mapping_id = key(data["id"])
    revision = integer(data["revision"], "revision", 0)
    result = {"id": mapping_id, "revision": revision, "materials": {},
              "locations": {}, "units": {}}
    for section in ("materials", "locations"):
        value = data[section]
        if not isinstance(value, dict):
            fail("input", "mapping section must be an object", section=section)
        for external, internal in value.items():
            if not isinstance(external, str) or not external.strip() or len(external) > 96:
                fail("input", "invalid external code", section=section)
            result[section][external] = key(internal)
    units = data["units"]
    if not isinstance(units, dict):
        fail("input", "mapping units must be an object")
    for external, spec in units.items():
        if not isinstance(external, str) or not external.strip() or len(external) > 96:
            fail("input", "invalid external unit code")
        obj(spec, ("unit", "factor"), ("unit", "factor"))
        factor = amount(spec["factor"])
        result["units"][external] = {
            "unit": key(spec["unit"]),
            "factor": text(factor),
        }
    return result


class ManufacturingExchange:
    def __init__(self, journal, consumer):
        self.journal = journal
        self.consumer = consumer

    # ------------------------------------------------------------------ 映射
    def load_mapping_file(self, path):
        return self.load_mapping(decode(read_file(path)))

    def load_mapping(self, data):
        mapping = validate_mapping(data)
        payload = canonical(mapping)
        digest = fingerprint(mapping)
        with self.journal.transaction() as con:
            old = con.execute(
                "SELECT * FROM mfg_mappings WHERE id=? AND revision=?",
                (mapping["id"], mapping["revision"]),
            ).fetchone()
            if old:
                if old["digest"] != digest:
                    fail(
                        "conflict",
                        "mapping revision content is immutable",
                        id=mapping["id"],
                        revision=mapping["revision"],
                    )
                return {"id": mapping["id"], "revision": mapping["revision"],
                        "digest": digest, "existing": True}
            con.execute(
                "INSERT INTO mfg_mappings VALUES(?,?,?,?)",
                (mapping["id"], mapping["revision"], payload, digest),
            )
            self.journal.record(
                "mfg_mapping_loaded",
                mapping["id"],
                {"revision": mapping["revision"], "digest": digest},
            )
        return {"id": mapping["id"], "revision": mapping["revision"],
                "digest": digest, "existing": False}

    def _mapping_family(self, con, mapping_id):
        rows = con.execute(
            "SELECT * FROM mfg_mappings WHERE id=? ORDER BY revision DESC",
            (mapping_id,),
        ).fetchall()
        if not rows:
            return None, None
        latest = rows[0]
        return decode(latest["payload"]), latest

    def _convert_quantity(self, mapping, unit, quantity, where):
        spec = mapping["units"].get(unit)
        if spec is None:
            fail("unknown_unit", "unit code is not in mapping", unit=unit, **where)
        with localcontext() as ctx:
            ctx.prec = 40
            converted = quantity * Decimal(spec["factor"])
        return spec["unit"], amount(converted)

    def _convert(self, txn, mapping):
        """把单个事务节点转换为信封字典；失败抛 ChainError（可修复类）。"""
        body = txn["body"]
        source = txn["source"]
        envelope_id = composite_key("mfg", source, txn["external_id"])
        operation_id = composite_key(txn["external_id"], body.attrs["id"])
        where = {"transaction": txn["external_id"], "line": body.line}
        if body.local == "Receive":
            leaves = body.children
            unit, quantity = self._convert_quantity(
                mapping, leaves[0].attrs["unit"],
                amount(Decimal(leaves[0].text.strip())), where,
            )
            material = mapping["materials"].get(body.attrs["material"])
            if material is None:
                fail("unknown_material", "material code is not in mapping",
                     external_code=body.attrs["material"], **where)
            location = mapping["locations"].get(body.attrs["location"])
            if location is None:
                fail("unknown_location", "location code is not in mapping",
                     external_code=body.attrs["location"], **where)
            supplier = key(body.attrs["supplier"])
            certificate = body.attrs.get("certificate")
            operation = {
                "id": operation_id,
                "kind": "receive",
                "lot": key(body.attrs["lot"]),
                "material": material,
                "unit": unit,
                "quantity": text(quantity),
                "location": location,
                "supplier": supplier,
                "certificate": key(certificate) if certificate is not None else None,
            }
        else:
            inputs, outputs = [], []
            canonical_units = set()
            waste = None
            for member in body.children:
                qnode = member.children[0]
                unit, quantity = self._convert_quantity(
                    mapping,
                    qnode.attrs["unit"],
                    amount(Decimal(qnode.text.strip()),
                           allow_zero=member.local == "Waste"),
                    {"transaction": txn["external_id"], "line": member.line},
                )
                canonical_units.add(unit)
                if member.local == "Input":
                    inputs.append({"lot": key(member.attrs["lot"]),
                                   "quantity": text(quantity)})
                elif member.local == "Output":
                    location = mapping["locations"].get(member.attrs["location"])
                    if location is None:
                        fail("unknown_location", "location code is not in mapping",
                             external_code=member.attrs["location"],
                             transaction=txn["external_id"], line=member.line)
                    outputs.append({"lot": key(member.attrs["lot"]),
                                    "quantity": text(quantity),
                                    "location": location})
                else:
                    waste = quantity
            if len(canonical_units) != 1:
                fail("unit_mismatch",
                     "transform members must resolve to one canonical unit",
                     units=sorted(canonical_units), **where)
            operation = {
                "id": operation_id,
                "kind": "transform",
                "inputs": inputs,
                "outputs": outputs,
                "waste": text(waste),
                "process": key(body.attrs["process"]),
            }
        envelope = {
            "id": envelope_id,
            "source": source,
            "offset": txn["sequence"],
            "occurred_at": txn["at"],
            "operations": [operation],
        }
        # 复用协议层的完整校验，拒绝重复批次等问题，整事务不落地。
        from .protocol import Envelope

        Envelope(envelope)
        return envelope

    # ------------------------------------------------------------------ 采集
    def ingest_xml(self, path=None, content=None):
        raw = read_file(path) if path is not None else content
        if not isinstance(raw, str):
            fail("input", "manufacturing exchange must be UTF-8 text")
        file_digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        root = parse_xml(raw)  # 危险/结构外内容在此硬拒收，无任何库写入
        source, mapping_attr, version, txn_nodes = validate_exchange(root)
        received_at = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        )
        summary = {
            "file_digest": file_digest,
            "source": source,
            "namespace": version,
            "mapping": mapping_attr,
            "received": 0,
            "duplicates": 0,
            "quarantined": 0,
            "blocked": 0,
            "duplicate_file": False,
        }
        with self.journal.transaction() as con:
            if con.execute(
                "SELECT 1 FROM mfg_files WHERE digest=?", (file_digest,)
            ).fetchone():
                summary["duplicate_file"] = True
            else:
                seen_ids, seen_sequences = set(), set()
                for item in txn_nodes:
                    if item["external_id"] in seen_ids:
                        fail("conflict", "duplicate transaction id in exchange",
                             id=item["external_id"])
                    if item["sequence"] in seen_sequences:
                        fail("conflict", "duplicate transaction sequence in exchange",
                             sequence=item["sequence"])
                    seen_ids.add(item["external_id"])
                    seen_sequences.add(item["sequence"])
                self.consumer.register(source)
                mapping, mapping_row = self._mapping_family(con, mapping_attr)
                for item in txn_nodes:
                    txn = {
                        "source": source,
                        "external_id": item["external_id"],
                        "sequence": item["sequence"],
                        "at": item["at"],
                        "body": item["body"],
                    }
                    node_dict = item["node"].to_dict()
                    content_digest = fingerprint(node_dict)
                    xml_location = (
                        f"/Exchange/Transaction[@id='{item['external_id']}']"
                        f"@line{item['node'].line}"
                    )
                    identity = con.execute(
                        "SELECT * FROM mfg_transactions WHERE source=? AND external_id=?",
                        (source, item["external_id"]),
                    ).fetchone()
                    if identity:
                        if identity["content_digest"] == content_digest:
                            summary["duplicates"] += 1
                            continue
                        con.execute(
                            "INSERT INTO mfg_quarantine VALUES(?,?,?,?,?,?,?,?,?)",
                            (
                                source,
                                item["external_id"],
                                file_digest,
                                content_digest,
                                canonical(node_dict),
                                xml_location,
                                canonical({"code": "content_conflict",
                                           "message": "transaction identity reused "
                                                      "with different content"}),
                                item["sequence"],
                                received_at,
                            ),
                        )
                        self.journal.record(
                            "mfg_transaction_quarantined",
                            f"{source}/{item['external_id']}",
                            {"content_digest": content_digest},
                        )
                        summary["quarantined"] += 1
                        continue
                    if mapping is None:
                        reason = {"code": "mapping_not_loaded",
                                  "message": "declared mapping is not available",
                                  "mapping": mapping_attr,
                                  "transaction": item["external_id"]}
                        envelope_id = None
                        mapping_revision = None
                        mapping_digest = None
                    else:
                        try:
                            envelope = self._convert(txn, mapping)
                        except ChainError as exc:
                            reason = exc.as_dict()
                            envelope_id = None
                            mapping_revision = mapping_row["revision"]
                            mapping_digest = mapping_row["digest"]
                        else:
                            reason = None
                            envelope_id = envelope["id"]
                            mapping_revision = mapping_row["revision"]
                            mapping_digest = mapping_row["digest"]
                    state = "blocked" if reason else "submitted"
                    con.execute(
                        "INSERT INTO mfg_transactions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            source,
                            item["external_id"],
                            item["sequence"],
                            mapping_attr,
                            mapping_revision,
                            mapping_digest,
                            xml_location,
                            content_digest,
                            canonical(node_dict),
                            state,
                            canonical(reason) if reason else None,
                            envelope_id,
                            file_digest,
                        ),
                    )
                    if reason:
                        summary["blocked"] += 1
                        self.journal.record(
                            "mfg_transaction_blocked",
                            f"{source}/{item['external_id']}",
                            reason,
                        )
                    else:
                        summary["received"] += 1
                        # drain=False：同文件多事务一次性进入原消费链，统一重试。
                        self.consumer.submit(envelope, drain=False)
                con.execute(
                    "INSERT INTO mfg_files VALUES(?,?,?,?,?)",
                    (file_digest, source, received_at, len(raw.encode("utf-8")),
                     len(txn_nodes)),
                )
                self.journal.record(
                    "mfg_file_ingested",
                    file_digest,
                    {"source": source, "transactions": len(txn_nodes),
                     "namespace": version, "mapping": mapping_attr},
                )
        drain = self.consumer.drain()
        summary["drain"] = drain
        if summary["duplicate_file"]:
            summary["transactions"] = [
                self._view(row) for row in self.journal.rows(
                    "SELECT * FROM mfg_transactions WHERE file_digest=? ORDER BY sequence",
                    (file_digest,),
                )
            ]
        else:
            summary["transactions"] = self.transactions(source=source)
        return summary

    def reprocess(self, source, external_id):
        """操作员补齐对照表后，重处理仍停在适配器层的选定事务。"""
        key(source)
        key(external_id)
        with self.journal.transaction() as con:
            row = con.execute(
                "SELECT * FROM mfg_transactions WHERE source=? AND external_id=?",
                (source, external_id),
            ).fetchone()
            if row is None:
                fail("missing", "manufacturing transaction not found")
            if row["state"] != "blocked":
                fail("state", "only adapter-blocked transactions can be reprocessed",
                     state=row["state"])
            mapping, mapping_row = self._mapping_family(con, row["mapping_id"])
            if mapping is None:
                fail("mapping_not_loaded", "declared mapping is not available",
                     mapping=row["mapping_id"])
            node = decode(row["payload"])
            body = node["children"][0]
            txn = {
                "source": source,
                "external_id": external_id,
                "sequence": row["sequence"],
                "at": self._node_at(node),
                "body": _DictNode(body),
            }
            try:
                envelope = self._convert(txn, mapping)
            except ChainError as exc:
                con.execute(
                    "UPDATE mfg_transactions SET reason=?, mapping_revision=?, "
                    "mapping_digest=? WHERE source=? AND external_id=?",
                    (canonical(exc.as_dict()), mapping_row["revision"],
                     mapping_row["digest"], source, external_id),
                )
                self.journal.record(
                    "mfg_transaction_blocked",
                    f"{source}/{external_id}",
                    exc.as_dict(),
                )
                return self._view(
                    con.execute(
                        "SELECT * FROM mfg_transactions WHERE source=? AND external_id=?",
                        (source, external_id),
                    ).fetchone()
                )
            self.consumer.submit(envelope, drain=False)
            con.execute(
                "UPDATE mfg_transactions SET state='submitted', reason=NULL, "
                "envelope_id=?, mapping_revision=?, mapping_digest=? "
                "WHERE source=? AND external_id=?",
                (envelope["id"], mapping_row["revision"], mapping_row["digest"],
                 source, external_id),
            )
            self.journal.record(
                "mfg_transaction_reprocessed",
                f"{source}/{external_id}",
                {"envelope": envelope["id"],
                 "mapping_revision": mapping_row["revision"],
                 "mapping_digest": mapping_row["digest"]},
            )
        drain = self.consumer.drain()
        return {"transaction": self.transaction(source, external_id), "drain": drain}

    @staticmethod
    def _node_at(node):
        return node["attrs"]["at"]

    # ------------------------------------------------------------------ 查询
    def _view(self, row):
        envelope_state = reason = None
        if row["envelope_id"]:
            envelope = self.journal.one(
                "SELECT state,reason FROM envelopes WHERE id=?", (row["envelope_id"],)
            )
            if envelope:
                envelope_state = envelope["state"]
                reason = decode(envelope["reason"]) if envelope["reason"] else None
        if reason is None and row["reason"]:
            reason = decode(row["reason"])
        return {
            "source": row["source"],
            "external_id": row["external_id"],
            "sequence": row["sequence"],
            "xml_location": row["xml_location"],
            "mapping": {
                "id": row["mapping_id"],
                "revision": row["mapping_revision"],
                "digest": row["mapping_digest"],
            },
            "state": envelope_state if row["state"] == "submitted" else row["state"],
            "adapter_state": row["state"],
            "envelope_id": row["envelope_id"],
            "reason": reason,
        }

    def transactions(self, source=None, state=None):
        sql = "SELECT * FROM mfg_transactions"
        args = []
        if source is not None:
            sql += " WHERE source=?"
            args.append(source)
        sql += " ORDER BY source,sequence"
        views = [self._view(row) for row in self.journal.rows(sql, tuple(args))]
        if state is not None:
            views = [row for row in views if row["state"] == state]
        return views

    def transaction(self, source, external_id):
        row = self.journal.one(
            "SELECT * FROM mfg_transactions WHERE source=? AND external_id=?",
            (source, external_id),
        )
        if row is None:
            fail("missing", "manufacturing transaction not found")
        return self._view(row)

    def unresolved(self):
        """未解决引用与未转换事务的统一队列。"""
        waiting = []
        for row in self.journal.rows(
            "SELECT * FROM mfg_transactions ORDER BY source,sequence"
        ):
            view = self._view(row)
            if view["state"] in {"blocked", "rejected", "pending"}:
                waiting.append(view)
        return waiting

    def quarantine(self):
        return [
            {
                "source": row["source"],
                "external_id": row["external_id"],
                "sequence": row["sequence"],
                "xml_location": row["xml_location"],
                "content_digest": row["content_digest"],
                "reason": decode(row["reason"]),
                "received_at": row["received_at"],
            }
            for row in self.journal.rows(
                "SELECT * FROM mfg_quarantine ORDER BY source,external_id,content_digest"
            )
        ]

    def mappings(self):
        return [
            {"id": row["id"], "revision": row["revision"], "digest": row["digest"]}
            for row in self.journal.rows(
                "SELECT * FROM mfg_mappings ORDER BY id,revision"
            )
        ]


class _DictNode:
    """让重处理路径复用同一转换代码的节点适配对象。"""

    def __init__(self, data):
        self.uri = NAMESPACE_BY_VERSION[data["ns"]]
        self.local = data["tag"]
        self.line = data["line"]
        self.attrs = dict(data["attrs"])
        self.text = data["text"]
        self.children = [_DictNode(child) for child in data["children"]]
