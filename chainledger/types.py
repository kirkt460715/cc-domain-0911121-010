import json
import hashlib
import re
from decimal import Decimal
from datetime import datetime, timezone


class ChainError(Exception):
    def __init__(self, code, message, **details):
        super().__init__(message)
        self.code = code
        self.details = details

    def as_dict(self):
        return {"code": self.code, "message": str(self), "details": self.details}


def fail(code, message, **details):
    raise ChainError(code, message, **details)


def key(value, field="id"):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}", value
    ):
        fail("input", "invalid identifier", field=field)
    return value


def amount(value, *, allow_zero=False):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        fail("input", "quantity must be exact decimal text or integer")
    try:
        value = Decimal(value)
    except Exception:
        fail("input", "invalid quantity")
    if (
        not value.is_finite()
        or value > Decimal("1e18")
        or value.as_tuple().exponent < -9
    ):
        fail("input", "quantity outside supported precision or magnitude")
    if value < 0 or (not allow_zero and value == 0):
        fail("input", "quantity must be positive")
    return value


def text(value):
    value = Decimal(value)
    result = format(value, "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


def integer(value, field, low=0, high=2**53 - 1):
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not low <= value <= high
    ):
        fail("input", "invalid bounded integer", field=field)
    return value


def timestamp(value):
    if not isinstance(value, str):
        fail("input", "timestamp must be ISO text")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        fail("input", "invalid ISO timestamp")
    if result.tzinfo is None or not 1970 <= result.year <= 2200:
        fail("input", "timestamp requires timezone and supported year")
    return (
        result.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def obj(value, fields=None, required=()):
    if not isinstance(value, dict):
        fail("input", "expected object")
    if set(required) - value.keys():
        fail("input", "missing fields", fields=sorted(set(required) - value.keys()))
    if fields is not None and value.keys() - set(fields):
        fail("input", "unknown fields", fields=sorted(value.keys() - set(fields)))
    return value


def array(value, minimum=0, maximum=10000):
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        fail("input", "invalid list size")
    return value


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def decode(value):
    def pairs(items):
        result = {}
        for k, v in items:
            if k in result:
                fail("input", "duplicate JSON key", field=k)
            result[k] = v
        return result

    def bad(token):
        fail("input", "nonfinite JSON token", token=token)

    try:
        return json.loads(value, object_pairs_hook=pairs, parse_constant=bad)
    except (ValueError, UnicodeError) as exc:
        fail("input", "invalid JSON", reason=str(exc))
