import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from .types import fail, canonical, fingerprint, decode

SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY,next_offset INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS envelopes(id TEXT PRIMARY KEY,source TEXT NOT NULL,offset INTEGER NOT NULL,
 payload TEXT NOT NULL,digest TEXT NOT NULL,state TEXT NOT NULL,reason TEXT,applied_order INTEGER,
 UNIQUE(source,offset));
CREATE TABLE IF NOT EXISTS lots(id TEXT PRIMARY KEY,material TEXT NOT NULL,unit TEXT NOT NULL,quantity TEXT NOT NULL,
 supplier TEXT,certificate TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS balances(lot TEXT NOT NULL,location TEXT NOT NULL,quantity TEXT NOT NULL,PRIMARY KEY(lot,location));
CREATE TABLE IF NOT EXISTS transformations(id TEXT PRIMARY KEY,process TEXT NOT NULL,waste TEXT NOT NULL,material TEXT NOT NULL,unit TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS inputs(transform_id TEXT NOT NULL,lot TEXT NOT NULL,quantity TEXT NOT NULL,PRIMARY KEY(transform_id,lot));
CREATE TABLE IF NOT EXISTS outputs(transform_id TEXT NOT NULL,lot TEXT NOT NULL,quantity TEXT NOT NULL,PRIMARY KEY(transform_id,lot));
CREATE TABLE IF NOT EXISTS edges(parent TEXT NOT NULL,child TEXT NOT NULL,transform_id TEXT NOT NULL,PRIMARY KEY(parent,child));
CREATE TABLE IF NOT EXISTS shipments(id TEXT PRIMARY KEY,lot TEXT NOT NULL,customer TEXT NOT NULL,quantity TEXT NOT NULL,returned TEXT NOT NULL,operation TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS holds(id TEXT PRIMARY KEY,lot TEXT NOT NULL,reason TEXT NOT NULL,active INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS scraps(id TEXT PRIMARY KEY,lot TEXT NOT NULL,quantity TEXT NOT NULL,reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY,envelope TEXT NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS adjustments(id TEXT PRIMARY KEY,target TEXT NOT NULL,replacement TEXT NOT NULL,reason TEXT NOT NULL,result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS certificates(id TEXT PRIMARY KEY,material TEXT NOT NULL,valid_from TEXT NOT NULL,valid_to TEXT NOT NULL,revoked INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS recalls(id TEXT PRIMARY KEY,payload TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS acknowledgements(recall TEXT NOT NULL,customer TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(recall,customer));
CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY,payload TEXT NOT NULL,digest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit(sequence INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,entity TEXT NOT NULL,payload TEXT NOT NULL,previous TEXT NOT NULL,digest TEXT NOT NULL);
"""


class Journal:
    def __init__(self, path=":memory:"):
        self.path = str(path)
        if self.path != ":memory:" and Path(path).is_symlink():
            fail("input", "database cannot be a symlink")
        self.connection = sqlite3.connect(
            self.path, timeout=5, isolation_level=None, check_same_thread=False
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.executescript(SCHEMA)
        self.lock = threading.RLock()
        self.depth = 0
        old = self.one("SELECT value FROM metadata WHERE key='version'")
        if old and old["value"] != "1":
            fail("schema", "unsupported journal version")
        self.connection.execute("INSERT OR IGNORE INTO metadata VALUES('version','1')")

    @contextmanager
    def transaction(self):
        with self.lock:
            depth = self.depth
            self.connection.execute(
                "BEGIN IMMEDIATE" if depth == 0 else f"SAVEPOINT nesting_{depth}"
            )
            self.depth += 1
            try:
                yield self.connection
            except BaseException:
                if depth == 0:
                    self.connection.execute("ROLLBACK")
                else:
                    self.connection.execute(f"ROLLBACK TO nesting_{depth}")
                    self.connection.execute(f"RELEASE nesting_{depth}")
                raise
            else:
                self.connection.execute(
                    "COMMIT" if depth == 0 else f"RELEASE nesting_{depth}"
                )
            finally:
                self.depth -= 1

    def rows(self, sql, args=()):
        with self.lock:
            return [dict(row) for row in self.connection.execute(sql, args)]

    def one(self, sql, args=()):
        rows = self.rows(sql, args)
        return rows[0] if rows else None

    def record(self, kind, entity, payload):
        old = self.one(
            "SELECT sequence,digest FROM audit ORDER BY sequence DESC LIMIT 1"
        )
        sequence = old["sequence"] + 1 if old else 1
        previous = old["digest"] if old else "0" * 64
        content = {
            "sequence": sequence,
            "kind": kind,
            "entity": entity,
            "payload": payload,
            "previous": previous,
        }
        self.connection.execute(
            "INSERT INTO audit VALUES(?,?,?,?,?,?)",
            (
                sequence,
                kind,
                entity,
                canonical(payload),
                previous,
                fingerprint(content),
            ),
        )

    def verify_audit(self):
        previous = "0" * 64
        count = 0
        for row in self.rows("SELECT * FROM audit ORDER BY sequence"):
            count += 1
            content = {
                "sequence": count,
                "kind": row["kind"],
                "entity": row["entity"],
                "payload": decode(row["payload"]),
                "previous": previous,
            }
            if (
                row["sequence"] != count
                or row["previous"] != previous
                or row["digest"] != fingerprint(content)
            ):
                fail("integrity", "audit chain mismatch", sequence=count)
            previous = row["digest"]
        return {"entries": count, "head": previous}

    def close(self):
        if self.depth:
            fail("state", "cannot close active transaction")
        self.connection.close()

    def backup(self, path):
        if Path(path).exists():
            fail("conflict", "backup path exists")
        with self.lock:
            if self.depth:
                fail("state", "backup requires no active transaction")
            target = sqlite3.connect(path)
            try:
                self.connection.backup(target)
            finally:
                target.close()
        return str(path)
