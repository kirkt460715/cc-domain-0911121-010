import csv
import io
import hashlib
import os
import tempfile
from pathlib import Path
from ..types import decode, fingerprint, fail, obj, array, key, integer
from .protocol import Envelope


class Exchange:
    def __init__(self, journal, consumer):
        self.journal = journal
        self.consumer = consumer

    def parse_jsonl(self, content):
        if not isinstance(content, str) or len(content.encode()) > 32000000:
            fail("input", "exchange input exceeds byte limit")
        records = []
        for line, raw in enumerate(content.splitlines(), 1):
            if not raw.strip():
                continue
            if len(records) >= 10000 or len(raw.encode()) > 1000000:
                fail("input", "exchange row limit exceeded", line=line)
            try:
                records.append(Envelope(decode(raw)))
            except Exception as exc:
                from ..types import ChainError

                if isinstance(exc, ChainError):
                    fail(
                        "input",
                        "invalid event in exchange",
                        line=line,
                        error=exc.as_dict(),
                    )
                raise
        return records

    def import_jsonl(self, content):
        envelopes = self.parse_jsonl(content)
        ids = set()
        offsets = set()
        for envelope in envelopes:
            if envelope.id in ids or (envelope.source, envelope.offset) in offsets:
                fail("conflict", "duplicate event identity within exchange")
            ids.add(envelope.id)
            offsets.add((envelope.source, envelope.offset))
        with self.journal.transaction():
            results = [
                self.consumer.submit(envelope.as_dict(), drain=False)
                for envelope in envelopes
            ]
            self.journal.record(
                "exchange_imported",
                "exchange",
                {
                    "count": len(results),
                    "digest": fingerprint([item.as_dict() for item in envelopes]),
                },
            )
        return {
            "received": len(results),
            "duplicates": sum(item["duplicate"] for item in results),
            "drain": self.consumer.drain(),
        }

    def export_source(self, source, start=0, end=None):
        key(source)
        integer(start, "start")
        if end is not None:
            integer(end, "end", start)
        if self.journal.one("SELECT id FROM sources WHERE id=?", (source,)) is None:
            fail("missing", "source not registered")
        rows = self.journal.rows(
            "SELECT * FROM envelopes WHERE source=? AND offset>=? ORDER BY offset",
            (source, start),
        )
        if end is not None:
            rows = [row for row in rows if row["offset"] < end]
        lines = [row["payload"] for row in rows]
        content = "\n".join(lines) + ("\n" if lines else "")
        return {
            "source": source,
            "start": start,
            "end": end,
            "count": len(rows),
            "sha256": hashlib.sha256(content.encode()).hexdigest(),
            "content": content,
        }

    def write_source(self, source, destination, start=0, end=None):
        exported = self.export_source(source, start, end)
        result = write_new(destination, exported["content"].encode())
        return {**result, "events": exported["count"], "sha256": exported["sha256"]}

    def offsets_csv(self):
        stream = io.StringIO(newline="")
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(
            [
                "source",
                "next_offset",
                "received",
                "applied",
                "blocked",
                "rejected",
                "skipped",
            ]
        )
        for source in self.journal.rows("SELECT * FROM sources ORDER BY id"):
            states = {
                row["state"]: row["n"]
                for row in self.journal.rows(
                    "SELECT state,COUNT(*) AS n FROM envelopes WHERE source=? GROUP BY state",
                    (source["id"],),
                )
            }
            writer.writerow(
                [
                    source["id"],
                    source["next_offset"],
                    sum(states.values()),
                    states.get("applied", 0),
                    states.get("blocked", 0),
                    states.get("rejected", 0),
                    states.get("skipped", 0),
                ]
            )
        return stream.getvalue()

    def compare_source(self, source, remote):
        obj(remote, {"source", "events"}, {"source", "events"})
        if remote["source"] != source:
            fail("input", "comparison source differs")
        remote_events = {}
        for value in array(remote["events"], 0, 100000):
            obj(value, {"offset", "digest"}, {"offset", "digest"})
            offset = integer(value["offset"], "offset")
            if offset in remote_events:
                fail("input", "duplicate remote offset")
            digest = value["digest"]
            if not isinstance(digest, str) or len(digest) != 64:
                fail("input", "invalid remote digest")
            remote_events[offset] = digest
        local = {
            row["offset"]: row["digest"]
            for row in self.journal.rows(
                "SELECT offset,digest FROM envelopes WHERE source=?", (source,)
            )
        }
        return {
            "source": source,
            "local_only": sorted(local.keys() - remote_events.keys()),
            "remote_only": sorted(remote_events.keys() - local.keys()),
            "conflicts": [
                offset
                for offset in sorted(local.keys() & remote_events.keys())
                if local[offset] != remote_events[offset]
            ],
        }


def read_file(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 32000000:
        fail("input", "input must be bounded regular file")
    try:
        return path.read_text(encoding="utf-8-sig")
    except UnicodeError:
        fail("input", "input must be UTF-8")


def write_new(path, content):
    path = Path(path)
    if path.exists() or path.is_symlink():
        fail("conflict", "destination exists")
    if not path.parent.is_dir():
        fail("input", "destination parent does not exist")
    fd, temporary = tempfile.mkstemp(prefix=".chain-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            fail("conflict", "destination appeared concurrently")
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {"path": str(path), "bytes": len(content)}
