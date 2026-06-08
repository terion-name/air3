"""air3 edge signed URL helpers.

These functions implement the air3 edge-gateway HMAC URL format. They are not
S3 SigV4 presigned URL helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import ipaddress
from posixpath import normpath
from urllib.parse import parse_qsl, unquote, urlsplit, urlunsplit

PARAM_EXPIRES = "expires"
PARAM_SIG = "sig"
PARAM_RANGE = "range"
PARAM_RESPONSE_CONTENT_TYPE = "response-content-type"
PARAM_RESPONSE_CONTENT_DISPOSITION = "response-content-disposition"


class EdgeSignError(ValueError):
    """Base error for edge signing failures."""


class InvalidSignatureError(EdgeSignError):
    def __init__(self) -> None:
        super().__init__("invalid signature")


class ExpiredSignatureError(EdgeSignError):
    def __init__(self) -> None:
        super().__init__("signature expired")


class UnsignedRangeError(EdgeSignError):
    def __init__(self) -> None:
        super().__init__("range header is not signed")


class RangeMismatchError(EdgeSignError):
    def __init__(self) -> None:
        super().__init__("range header does not match signed range")


@dataclass(frozen=True)
class Claims:
    method: str
    bucket: str
    key: str
    expires: datetime
    range: str = ""
    response_content_type: str = ""
    response_content_disposition: str = ""


def sign_url(
    *,
    method: str,
    base_url: str,
    bucket: str,
    key: str,
    secret: str,
    expires: datetime | int | float,
    range: str = "",
    response_content_type: str = "",
    response_content_disposition: str = "",
) -> str:
    """Return an air3 edge signed object URL."""
    if secret == "":
        raise EdgeSignError("signing secret is required")
    claims = _claims_from_input(
        method=method,
        bucket=bucket,
        key=key,
        expires=expires,
        range=range,
        response_content_type=response_content_type,
        response_content_disposition=response_content_disposition,
    )
    parsed = urlsplit(base_url)
    if not parsed.scheme or not parsed.netloc:
        raise EdgeSignError("base url must be absolute")

    object_path = _append_object_path(unquote(parsed.path), bucket, key)
    query = _collect_query(parsed.query)
    _set_query_value(query, PARAM_EXPIRES, str(_unix_seconds(expires)))
    if range:
        _set_query_value(query, PARAM_RANGE, range)
    if response_content_type:
        _set_query_value(query, PARAM_RESPONSE_CONTENT_TYPE, response_content_type)
    if response_content_disposition:
        _set_query_value(query, PARAM_RESPONSE_CONTENT_DISPOSITION, response_content_disposition)
    _set_query_value(query, PARAM_SIG, _signature_hex(claims, secret))

    return urlunsplit((parsed.scheme, parsed.netloc, _escape_url_path(object_path), _encode_query(query), ""))


def verify_url(
    *,
    method: str,
    url: str,
    secret: str,
    now: datetime | int | float,
    range: str = "",
) -> Claims:
    """Validate an air3 edge signed URL and return its signed claims."""
    claims, sig = _claims_from_url(method, url)
    if secret == "":
        raise EdgeSignError("signing secret is required")
    if sig == "":
        raise InvalidSignatureError()
    if _unix_seconds(claims.expires) <= _unix_seconds(now):
        raise ExpiredSignatureError()
    if not _constant_time_hex_equal(sig, _signature_hex(claims, secret)):
        raise InvalidSignatureError()

    range_header = range.strip()
    if range_header:
        if not claims.range:
            raise UnsignedRangeError()
        if claims.range != range_header:
            raise RangeMismatchError()
    return claims


def canonical_string(claims: Claims) -> str:
    """Return the exact newline-delimited text covered by the edge signature."""
    return "\n".join(
        [
            claims.method.upper(),
            claims.bucket,
            claims.key,
            str(_unix_seconds(claims.expires)),
            claims.range,
            claims.response_content_type,
            claims.response_content_disposition,
        ]
    )


def _claims_from_input(
    *,
    method: str,
    bucket: str,
    key: str,
    expires: datetime | int | float,
    range: str = "",
    response_content_type: str = "",
    response_content_disposition: str = "",
) -> Claims:
    normalized_method = method.strip().upper()
    if normalized_method not in {"GET", "HEAD"}:
        raise EdgeSignError("method must be GET or HEAD")
    _validate_bucket(bucket)
    _validate_key(key)
    expires_unix = _unix_seconds(expires)
    if expires_unix <= 0:
        raise EdgeSignError("expiration is required")
    return Claims(
        method=normalized_method,
        bucket=bucket,
        key=key,
        expires=datetime.fromtimestamp(expires_unix, tz=timezone.utc),
        range=range,
        response_content_type=response_content_type,
        response_content_disposition=response_content_disposition,
    )


def _claims_from_url(method: str, raw_url: str) -> tuple[Claims, str]:
    parsed = urlsplit(raw_url)
    bucket, key = _object_from_path(parsed.path)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    expires_text = _query_get(query, PARAM_EXPIRES)
    if not expires_text:
        raise EdgeSignError("expires query parameter is required")
    expires = _parse_unix_seconds(expires_text)
    claims = _claims_from_input(
        method=method,
        bucket=bucket,
        key=key,
        expires=expires,
        range=_query_get(query, PARAM_RANGE),
        response_content_type=_query_get(query, PARAM_RESPONSE_CONTENT_TYPE),
        response_content_disposition=_query_get(query, PARAM_RESPONSE_CONTENT_DISPOSITION),
    )
    return claims, _query_get(query, PARAM_SIG)


def _signature_hex(claims: Claims, secret: str) -> str:
    return hmac.new(secret.encode(), canonical_string(claims).encode(), hashlib.sha256).hexdigest()


def _constant_time_hex_equal(supplied_hex: str, expected_hex: str) -> bool:
    try:
        supplied = bytes.fromhex(supplied_hex)
    except ValueError:
        return False
    expected = bytes.fromhex(expected_hex)
    return hmac.compare_digest(supplied, expected)


def _append_object_path(base_path: str, bucket: str, key: str) -> str:
    parts = [_trim_slashes(base_path), _go_path_escape(bucket)]
    parts.extend(_go_path_escape(part) for part in key.split("/"))
    return "/" + "/".join(part for part in parts if part)


def _object_from_path(escaped_path: str) -> tuple[str, str]:
    cleaned = normpath("/" + escaped_path).lstrip("/")
    if cleaned in {"", "."}:
        raise EdgeSignError("signed url path must include bucket and key")
    parts = cleaned.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise EdgeSignError("signed url path must include bucket and key")
    return unquote(parts[0]), unquote(parts[1])


def _query_get(values: list[tuple[str, str]], key: str) -> str:
    for value_key, value in values:
        if value_key == key:
            return value
    return ""


def _collect_query(raw_query: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for key, value in parse_qsl(raw_query, keep_blank_values=True):
        out.setdefault(key, []).append(value)
    return out


def _set_query_value(values: dict[str, list[str]], key: str, value: str) -> None:
    values[key] = [value]


def _encode_query(values: dict[str, list[str]]) -> str:
    parts: list[str] = []
    for key in sorted(values):
        for value in values[key]:
            parts.append(f"{_go_query_escape(key)}={_go_query_escape(value)}")
    return "&".join(parts)


def _go_path_escape(value: str) -> str:
    return _percent_encode(value, safe=b"-._~$&+=:@", encoded_space="%20")


def _escape_url_path(value: str) -> str:
    return _percent_encode(value, safe=b"-._~$&+,;=:@/", encoded_space="%20")


def _go_query_escape(value: str) -> str:
    return _percent_encode(value, safe=b"-._~", encoded_space="+")


def _percent_encode(value: str, *, safe: bytes, encoded_space: str) -> str:
    out: list[str] = []
    for byte in value.encode():
        if byte == 0x20:
            out.append(encoded_space)
        elif (0x30 <= byte <= 0x39) or (0x41 <= byte <= 0x5A) or (0x61 <= byte <= 0x7A) or byte in safe:
            out.append(chr(byte))
        else:
            out.append(f"%{byte:02X}")
    return "".join(out)


def _trim_slashes(value: str) -> str:
    return value.strip("/")


def _unix_seconds(value: datetime | int | float) -> int:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp())
    return int(value)


def _parse_unix_seconds(text: str) -> int:
    if not text.isdigit():
        raise EdgeSignError("expires query parameter must be unix seconds")
    value = int(text)
    if value <= 0:
        raise EdgeSignError("expires query parameter must be positive")
    return value


def _validate_bucket(bucket: str) -> None:
    import re

    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise EdgeSignError("invalid ticket: bucket must be a valid DNS-style S3 bucket name")
    if ".." in bucket or ".-" in bucket or "-." in bucket:
        raise EdgeSignError("invalid ticket: bucket contains invalid dot/hyphen placement")
    try:
        ipaddress.ip_address(bucket)
    except ValueError:
        return
    raise EdgeSignError("invalid ticket: bucket must not look like an IP address")


def _validate_key(key: str) -> None:
    if key == "":
        raise EdgeSignError("invalid ticket: key is required")
    if len(key) > 1024:
        raise EdgeSignError("invalid ticket: key is too long")
    if key.startswith("/") or key.endswith("/"):
        raise EdgeSignError("invalid ticket: key must not start or end with slash")
    for part in key.split("/"):
        if part in {"", ".", ".."}:
            raise EdgeSignError("invalid ticket: key must not contain empty or traversal path segments")
    if any(ord(ch) == 0 or (ord(ch) < 32) or (127 <= ord(ch) <= 159) for ch in key):
        raise EdgeSignError("invalid ticket: key must not contain control characters")


__all__ = [
    "Claims",
    "EdgeSignError",
    "ExpiredSignatureError",
    "InvalidSignatureError",
    "RangeMismatchError",
    "UnsignedRangeError",
    "canonical_string",
    "sign_url",
    "verify_url",
]
