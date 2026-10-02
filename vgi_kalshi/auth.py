"""Optional Kalshi API-key authentication (RSA-PSS request signing).

Authentication is **not** required by this worker and is not a throughput win at
Kalshi's entry tier — see the rate-limit note in the README. It exists so an
account with a higher tier can use it, and so a deployment can trade the
undocumented per-IP allowance for a documented one.

Credentials arrive as a DuckDB secret rather than an ATTACH option, because an
ATTACH option string is visible in ``duckdb_databases()`` and in anything that
logs the statement, and one of these values is an RSA private key::

    CREATE SECRET kalshi (
        TYPE kalshi,
        key_id '9f8e7d6c-...',
        private_key '-----BEGIN RSA PRIVATE KEY-----
    ...
    -----END RSA PRIVATE KEY-----'
    );

``private_key`` is marked redacted, so ``duckdb_secrets()`` shows it masked.
The secret may have any name, or none. When several ``kalshi`` secrets exist —
a production key and a demo key, say — ``SCOPE`` picks between them by the API
base URL being called::

    CREATE SECRET kalshi_demo (TYPE kalshi, key_id '...', private_key '...',
                               SCOPE 'https://demo-api.kalshi.co');

Signing stays read-only: it adds three request headers to a ``GET`` and nothing
else. The chokepoint in :mod:`vgi_kalshi.kalshi_api` is still the only place an
HTTP call is made.
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from vgi.catalog.secret_type import SecretTypeSpec

#: The DuckDB secret type this worker registers at ATTACH.
SECRET_TYPE = "kalshi"

#: Secret keys. ``key_id`` is the API Key ID from Kalshi's dashboard; it is not
#: itself sensitive, but it is useless apart from the key so they live together.
KEY_ID = "key_id"
PRIVATE_KEY = "private_key"

#: Kalshi signs the full path from the API root, so the base URL's path prefix
#: is part of the signed string even though it never varies.
API_ROOT_PATH = "/trade-api/v2"

SECRET_SPEC = SecretTypeSpec(
    name=SECRET_TYPE,
    description=(
        "Kalshi API credentials for authenticated access. Optional — this worker's "
        "whole surface is public without them. Provide key_id (the API Key ID) and "
        "private_key (its RSA private key, PEM)."
    ),
    schema=pa.schema(
        [
            pa.field(KEY_ID, pa.string()),
            pa.field(PRIVATE_KEY, pa.string(), metadata={"redact": "true"}),
        ]
    ),
)


class KalshiAuthError(RuntimeError):
    """A credential was supplied but cannot be used to sign."""


@dataclass(slots=True, frozen=True)
class Credentials:
    """A loaded Kalshi API key, ready to sign requests."""

    key_id: str
    private_key: rsa.RSAPrivateKey

    def headers(self, method: str, path: str, *, now_ms: int | None = None) -> dict[str, str]:
        """The three signed headers for one request.

        Kalshi signs ``timestamp + METHOD + path`` where ``path`` is the full
        path from the API root **without** the query string, so two calls to the
        same endpoint with different parameters share a signature input. The
        signature is RSA-PSS over SHA-256 with a digest-length salt, base64'd.

        Args:
            method: HTTP method, uppercased by the caller's convention.
            path: Path below the API root, with a leading slash.
            now_ms: Millisecond timestamp to sign; defaults to now. Injectable
                so a test can assert an exact signature.

        Returns:
            The ``KALSHI-ACCESS-*`` headers to merge into the request.
        """
        timestamp = str(int(time.time() * 1000) if now_ms is None else now_ms)
        message = f"{timestamp}{method}{API_ROOT_PATH}{path}".encode()
        signature = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        }


def load(key_id: str, private_key_pem: str) -> Credentials:
    """Parse a PEM private key into :class:`Credentials`.

    Raises:
        KalshiAuthError: The PEM is unreadable, encrypted, or not an RSA key.
            Kalshi's scheme is RSA-PSS specifically, so an EC or Ed25519 key is
            a configuration mistake worth naming rather than a signing failure
            later, on every request.
    """
    if not key_id or not private_key_pem:
        raise KalshiAuthError(f"a {SECRET_TYPE!r} secret needs both {KEY_ID!r} and {PRIVATE_KEY!r}")
    try:
        key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    except TypeError as exc:  # password-protected key
        raise KalshiAuthError(
            f"the {PRIVATE_KEY!r} in the {SECRET_TYPE!r} secret is encrypted; "
            "Kalshi API keys are stored unencrypted, so supply the key as issued"
        ) from exc
    except (ValueError, UnsupportedAlgorithm) as exc:
        raise KalshiAuthError(
            f"could not read the {PRIVATE_KEY!r} in the {SECRET_TYPE!r} secret as a PEM private key: {exc}"
        ) from exc
    if not isinstance(key, rsa.RSAPrivateKey):
        raise KalshiAuthError(
            f"Kalshi signs with RSA-PSS, but the {PRIVATE_KEY!r} in the {SECRET_TYPE!r} "
            f"secret is a {type(key).__name__}"
        )
    return Credentials(key_id=key_id, private_key=key)


def _select(secrets: dict[str, dict[str, Any]], scope: str | None) -> dict[str, Any] | None:
    """The ``kalshi`` secret that applies to ``scope``, from resolved DuckDB secrets.

    The framework keys resolved secrets by each secret's *name* — whatever
    followed ``CREATE SECRET``, or ``__default_kalshi`` for an unnamed one — not
    by its type. Looking one up by the type string therefore only worked for a
    secret that happened to be named ``kalshi``; any other name, including the
    unnamed form this module's own error message suggested, was ignored, and
    in ``auto`` mode the query silently ran unsigned.

    Selection goes by each secret's ``type`` field instead, and by its ``SCOPE``
    when several are present: the longest scope that prefixes the API base URL
    wins, and an unscoped secret is the fallback.
    """
    selector = getattr(secrets, "for_scope_of_type", None)
    if selector is not None:
        found = selector(scope or "", SECRET_TYPE)
        if found:
            return dict(found)
    # A plain dict (tests, or an older framework): keyed by type, or carrying
    # a `type` field.
    by_name = secrets.get(SECRET_TYPE)
    if by_name:
        return by_name
    typed = [v for v in secrets.values() if isinstance(v, dict) and _text(v.get("type")) == SECRET_TYPE]
    return typed[0] if typed else None


def _text(raw: Any) -> str:
    """A resolved secret field as text; they arrive as Arrow scalars, tests pass plain values."""
    value = raw.as_py() if hasattr(raw, "as_py") else raw
    return "" if value is None else str(value)


def from_secrets(secrets: dict[str, dict[str, Any]] | None, scope: str | None = None) -> Credentials | None:
    """Build credentials from resolved DuckDB secrets, or None for public access.

    A missing secret is the normal case, not an error: the market-data surface
    is public, so the worker simply stops signing. A secret that is *present*
    but unusable does raise — someone asked for authentication and would
    otherwise silently get anonymous access on a different rate limit.

    Args:
        secrets: Resolved secrets, as the framework hands them to a function.
        scope: The API base URL being called, used to choose between several
            ``kalshi`` secrets by their ``SCOPE``.
    """
    values = _select(secrets, scope) if secrets else None
    if not values:
        return None
    return load(_text(values.get(KEY_ID)), _text(values.get(PRIVATE_KEY)))


# ---------------------------------------------------------------------------
# Attach-time policy
# ---------------------------------------------------------------------------

#: What ATTACH asked to happen when no credential resolves. The mode travels as
#: the catalog's attach bytes, which the framework hands back to every function.
AUTO = "auto"
REQUIRED = "required"
OFF = "off"
MODES = (AUTO, REQUIRED, OFF)


def mode_of(attach_opaque_data: bytes | None) -> str:
    """The auth mode ATTACH selected, defaulting to :data:`AUTO`.

    Anything unrecognised reads as ``auto``: the mode is validated at ATTACH,
    so a surprise here means an older client, and degrading to the default
    behaviour beats failing every query.
    """
    if not attach_opaque_data:
        return AUTO
    mode = attach_opaque_data.decode(errors="replace").strip().lower()
    return mode if mode in MODES else AUTO


def for_call(
    secrets: dict[str, dict[str, Any]] | None, attach_opaque_data: bytes | None = None
) -> Credentials | None:
    """The credentials to sign this call with, honouring the ATTACH auth mode.

    Raises:
        KalshiAuthError: ``auth => 'required'`` was requested and no usable
            credential resolved. Failing here is the point of that mode —
            otherwise the query silently succeeds against public data on a
            different rate limit than the caller was counting on.
    """
    mode = mode_of(attach_opaque_data)
    if mode == OFF:
        return None
    # Imported here: kalshi_api imports Credentials from this module.
    from vgi_kalshi.kalshi_api import base_url

    credentials = from_secrets(secrets, base_url())
    if credentials is None and mode == REQUIRED:
        raise KalshiAuthError(
            f"ATTACH requested auth => 'required' but no {SECRET_TYPE!r} secret resolved; "
            f"CREATE SECRET (TYPE {SECRET_TYPE}, {KEY_ID} '...', {PRIVATE_KEY} '...') first, "
            "or attach with auth => 'auto' to allow public access"
        )
    return credentials
