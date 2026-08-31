"""Optional API-key authentication: signing, secret handling, and the ATTACH mode.

The signature is verified here against the public half of a throwaway key, so
these tests check the scheme itself — not merely that some bytes were produced —
without needing a Kalshi account.
"""

from __future__ import annotations

import base64

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from vgi_kalshi import auth, kalshi_api


@pytest.fixture(scope="module")
def keypair() -> tuple[str, rsa.RSAPublicKey]:
    """A throwaway RSA key, as PEM plus the public half to verify against."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    return pem, key.public_key()


class TestSigning:
    def test_signature_verifies_against_the_public_key(self, keypair: tuple[str, rsa.RSAPublicKey]) -> None:
        """Kalshi signs timestamp + METHOD + full path, RSA-PSS over SHA-256."""
        pem, public = keypair
        creds = auth.load("key-1", pem)
        headers = creds.headers("GET", "/markets/ABC", now_ms=1703123456789)

        assert headers["KALSHI-ACCESS-KEY"] == "key-1"
        assert headers["KALSHI-ACCESS-TIMESTAMP"] == "1703123456789"
        public.verify(
            base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
            b"1703123456789GET/trade-api/v2/markets/ABC",
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )

    def test_signed_path_includes_the_api_root(self, keypair: tuple[str, rsa.RSAPublicKey]) -> None:
        """Signing the bare '/markets' rather than '/trade-api/v2/markets' is a 401."""
        assert auth.API_ROOT_PATH == "/trade-api/v2"

    def test_query_string_is_not_signed(self, keypair: tuple[str, rsa.RSAPublicKey]) -> None:
        """Kalshi signs the path without parameters, so paging must not change it."""
        pem, public = keypair
        creds = auth.load("key-1", pem)
        message = b"1700000000000GET/trade-api/v2/markets"
        signature = base64.b64decode(
            creds.headers("GET", "/markets", now_ms=1700000000000)["KALSHI-ACCESS-SIGNATURE"]
        )
        public.verify(
            signature,
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )

    def test_each_call_gets_a_fresh_timestamp(self, keypair: tuple[str, rsa.RSAPublicKey]) -> None:
        pem, _ = keypair
        creds = auth.load("key-1", pem)
        first = creds.headers("GET", "/series")["KALSHI-ACCESS-TIMESTAMP"]
        assert int(first) > 1_700_000_000_000


class TestCredentialErrors:
    """A credential that was supplied but cannot be used must say so loudly."""

    def test_garbage_pem_is_named(self) -> None:
        with pytest.raises(auth.KalshiAuthError, match="PEM private key"):
            auth.load("key-1", "not a pem")

    def test_a_non_rsa_key_is_named(self) -> None:
        """Kalshi's scheme is RSA-PSS, so an Ed25519 key can never work."""
        from cryptography.hazmat.primitives.asymmetric import ed25519

        pem = (
            ed25519.Ed25519PrivateKey.generate()
            .private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            .decode()
        )
        with pytest.raises(auth.KalshiAuthError, match="RSA-PSS"):
            auth.load("key-1", pem)

    def test_half_a_credential_is_an_error(self, keypair: tuple[str, rsa.RSAPublicKey]) -> None:
        pem, _ = keypair
        with pytest.raises(auth.KalshiAuthError):
            auth.load("", pem)


class TestSecretResolution:
    def test_no_secret_means_public_access(self) -> None:
        """The market-data surface is public, so an absent credential is normal."""
        assert auth.from_secrets(None) is None
        assert auth.from_secrets({}) is None

    def test_secret_builds_credentials(self, keypair: tuple[str, rsa.RSAPublicKey]) -> None:
        pem, _ = keypair
        creds = auth.from_secrets({auth.SECRET_TYPE: {auth.KEY_ID: "k", auth.PRIVATE_KEY: pem}})
        assert creds is not None
        assert creds.key_id == "k"

    def test_private_key_is_declared_redacted(self) -> None:
        """`duckdb_secrets()` must mask the key; the field metadata is what does it."""
        field = auth.SECRET_SPEC.schema.field(auth.PRIVATE_KEY)
        assert (field.metadata or {}).get(b"redact") == b"true"


class TestAttachMode:
    @pytest.mark.parametrize(
        ("raw", "expected"), [(b"auto", "auto"), (b"required", "required"), (b"off", "off")]
    )
    def test_mode_round_trips_through_attach_bytes(self, raw: bytes, expected: str) -> None:
        assert auth.mode_of(raw) == expected

    @pytest.mark.parametrize("raw", [None, b"", b"nonsense"])
    def test_unknown_mode_degrades_to_auto(self, raw: bytes | None) -> None:
        """An older client sending nothing must not fail every query."""
        assert auth.mode_of(raw) == auth.AUTO

    def test_off_ignores_a_present_secret(self, keypair: tuple[str, rsa.RSAPublicKey]) -> None:
        pem, _ = keypair
        secrets = {auth.SECRET_TYPE: {auth.KEY_ID: "k", auth.PRIVATE_KEY: pem}}
        assert auth.for_call(secrets, b"off") is None

    def test_required_fails_when_no_secret_resolves(self) -> None:
        """The whole point of 'required': never silently fall back to anonymous."""
        with pytest.raises(auth.KalshiAuthError, match="no 'kalshi' secret resolved"):
            auth.for_call(None, b"required")

    def test_auto_falls_back_to_public(self) -> None:
        assert auth.for_call(None, b"auto") is None


class TestRequestSigning:
    """The headers have to reach the wire, on every attempt."""

    @staticmethod
    def _capture(seen: list[httpx.Request], statuses: list[int]) -> httpx.Client:
        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(statuses[min(len(seen) - 1, len(statuses) - 1)], json={"series": []})

        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_unauthenticated_requests_carry_no_auth_headers(self) -> None:
        seen: list[httpx.Request] = []
        kalshi_api.series_list(client=self._capture(seen, [200]))
        assert "KALSHI-ACCESS-KEY" not in seen[0].headers

    def test_credentials_sign_the_request(self, keypair: tuple[str, rsa.RSAPublicKey]) -> None:
        pem, _ = keypair
        seen: list[httpx.Request] = []
        kalshi_api.series_list(client=self._capture(seen, [200]), credentials=auth.load("k", pem))
        assert seen[0].headers["KALSHI-ACCESS-KEY"] == "k"
        assert seen[0].headers["KALSHI-ACCESS-SIGNATURE"]

    def test_a_retry_is_signed_again(
        self, keypair: tuple[str, rsa.RSAPublicKey], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A signature covers a timestamp, so replaying one after backoff is a 401."""
        monkeypatch.setattr(kalshi_api.time, "sleep", lambda _s: None)
        pem, _ = keypair
        seen: list[httpx.Request] = []
        kalshi_api.series_list(client=self._capture(seen, [429, 200]), credentials=auth.load("k", pem))
        assert len(seen) == 2
        signatures = {r.headers["KALSHI-ACCESS-SIGNATURE"] for r in seen}
        assert len(signatures) == 2, "the retry replayed the first request's signature"
