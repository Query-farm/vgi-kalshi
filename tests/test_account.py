"""The perps account functions: signed, allowlisted, never anonymous, exact.

Offline: a mock transport stands in for Kalshi, so these check the requests the
worker sends and how it shapes the answers, without an account. The payloads
mirror real `/margin/*` responses (values changed).
"""

from __future__ import annotations

import base64
from decimal import Decimal
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from vgi_kalshi import auth, kalshi_api
from vgi_kalshi.schemas import (
    MARGIN_BALANCE_SCHEMA,
    MARGIN_FUNDING_SCHEMA,
    MARGIN_POSITION_SCHEMA,
    MARGIN_RISK_SCHEMA,
    batch_from_rows,
    flatten_margin_balance,
    flatten_margin_funding,
    flatten_margin_risk,
)

POSITIONS = {
    "positions": [
        {
            "entry_price": "3.8643",
            "fees": "0.2773",
            "is_portfolio": False,
            "margin_used": "184.5318",
            "market_ticker": "KXGOLDPERP",
            "position": "-996.00",
            "roe": -5.07565633673979,
            "subaccount": 85,
            "subaccount_type": "isolated",
            "unrealized_pnl": "-9.3662",
        }
    ]
}
BALANCE = {
    "settled_funds": "1100.000000",
    "subaccount_balances": [
        {
            "account_equity": "0.000000",
            "subaccount": 0,
            "subaccount_type": "cross",
            "maintenance_margin": "0.0000",
        },
        {
            "account_equity": "1090.356500",
            "available_balance": "0.000000",
            "initial_margin": "233.3708",
            "maintenance_margin": "184.5318",
            "position_value": "-3858.304800",
            "resting_orders_margin": "0.0000",
            "subaccount": 85,
            "subaccount_type": "isolated",
        },
    ],
}
RISK = {
    "account_leverage": 20.9086,
    "positions": [
        {
            "estimated_liquidation_price": "4.9065",
            "is_portfolio": False,
            "maintenance_margin_required": "184.5318",
            "mark_price": "3.8738",
            "market_ticker": "KXGOLDPERP",
            "position": "-996.00",
            "position_leverage": 20.9086,
            "position_notional": "3858.3048",
            "subaccount": 85,
            "subaccount_type": "isolated",
        }
    ],
    "total_maintenance_margin": "184.5318",
    "total_position_notional": "3858.3048",
}
FUNDING = {
    "funding_history": [
        {
            "market_ticker": "KXGOLDPERP",
            "funding_time": "2026-11-16T15:00:00Z",
            "funding_rate": 0.000132,
            "mark_price": "3.8711",
            "funding_amount": "0.508962",
            "quantity": "-996.00",
            "subaccount_number": 85,
            "subaccount_type": "isolated",
        }
    ]
}
BODIES = {
    "/trade-api/v2/margin/positions": POSITIONS,
    "/trade-api/v2/margin/balance": BALANCE,
    "/trade-api/v2/margin/risk": RISK,
    "/trade-api/v2/margin/funding_history": FUNDING,
}


@pytest.fixture(scope="module")
def creds() -> auth.Credentials:
    key = ed25519.Ed25519PrivateKey.generate()
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    return auth.load("key-ed", pem)


@pytest.fixture
def recorder() -> tuple[httpx.Client, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=BODIES[request.url.path])

    return httpx.Client(transport=httpx.MockTransport(handler)), seen


CALLS: list[tuple[str, Any]] = [
    ("margin_positions", lambda c, cr: kalshi_api.margin_positions(client=c, credentials=cr)),
    ("margin_balance", lambda c, cr: kalshi_api.margin_balance(client=c, credentials=cr)),
    ("margin_risk", lambda c, cr: kalshi_api.margin_risk(client=c, credentials=cr)),
    (
        "margin_funding_history",
        lambda c, cr: kalshi_api.margin_funding_history("2026-11-01", "2026-11-16", client=c, credentials=cr),
    ),
]


class TestNeverAnonymous:
    @pytest.mark.parametrize(("name", "call"), CALLS, ids=[c[0] for c in CALLS])
    def test_refuses_without_credentials_and_sends_nothing(
        self, name: str, call: Any, recorder: tuple[httpx.Client, list[httpx.Request]]
    ) -> None:
        client, seen = recorder
        with pytest.raises(kalshi_api.KalshiCredentialsRequired, match=name):
            call(client, None)
        assert seen == []


class TestSignedRequests:
    @pytest.mark.parametrize(("name", "call"), CALLS, ids=[c[0] for c in CALLS])
    def test_each_is_a_signed_get_of_its_path(
        self,
        name: str,
        call: Any,
        creds: auth.Credentials,
        recorder: tuple[httpx.Client, list[httpx.Request]],
    ) -> None:
        client, seen = recorder
        call(client, creds)
        (request,) = seen
        assert request.method == "GET"
        assert request.url.path == f"/trade-api/v2/margin/{name.removeprefix('margin_')}"
        assert request.headers["KALSHI-ACCESS-KEY"] == "key-ed"
        message = f"{request.headers['KALSHI-ACCESS-TIMESTAMP']}GET{request.url.path}".encode()
        creds.private_key.public_key().verify(
            base64.b64decode(request.headers["KALSHI-ACCESS-SIGNATURE"]), message
        )  # type: ignore[union-attr]

    def test_funding_history_sends_dates_and_ticker(
        self, creds: auth.Credentials, recorder: tuple[httpx.Client, list[httpx.Request]]
    ) -> None:
        client, seen = recorder
        kalshi_api.margin_funding_history(
            "2026-11-01", "2026-11-16", ticker="KXGOLDPERP", client=client, credentials=creds
        )
        params = dict(seen[0].url.params)
        assert params == {"start_date": "2026-11-01", "end_date": "2026-11-16", "ticker": "KXGOLDPERP"}

    def test_positions_omits_unset_filters(
        self, creds: auth.Credentials, recorder: tuple[httpx.Client, list[httpx.Request]]
    ) -> None:
        client, seen = recorder
        kalshi_api.margin_positions(client=client, credentials=creds)
        assert dict(seen[0].url.params) == {}


class TestShaping:
    def test_positions_keep_exact_decimals(self) -> None:
        batch = batch_from_rows(POSITIONS["positions"], MARGIN_POSITION_SCHEMA).to_pylist()[0]
        assert batch["position"] == Decimal("-996.00")
        assert batch["entry_price"] == Decimal("3.8643")
        assert batch["subaccount"] == 85
        assert batch["roe"] == pytest.approx(-5.0756563)

    def test_six_decimal_balances_are_not_nulled(self) -> None:
        """Kalshi emits up to 6dp on account money; a 4dp column would turn these into NULL."""
        rows = batch_from_rows(flatten_margin_balance(BALANCE), MARGIN_BALANCE_SCHEMA).to_pylist()
        isolated = next(r for r in rows if r["subaccount"] == 85)
        assert isolated["account_equity"] == Decimal("1090.356500")
        assert isolated["position_value"] == Decimal("-3858.304800")
        assert {r["settled_funds"] for r in rows} == {Decimal("1100.000000")}
        # A field Kalshi omitted is NULL, not zero.
        assert next(r for r in rows if r["subaccount"] == 0)["initial_margin"] is None

    def test_risk_repeats_account_totals_on_each_position(self) -> None:
        (row,) = batch_from_rows(flatten_margin_risk(RISK), MARGIN_RISK_SCHEMA).to_pylist()
        assert row["estimated_liquidation_price"] == Decimal("4.9065")
        assert row["account_leverage"] == pytest.approx(20.9086)
        assert row["total_position_notional"] == Decimal("3858.3048")

    def test_funding_unifies_the_subaccount_column(self) -> None:
        (row,) = batch_from_rows(
            flatten_margin_funding(FUNDING["funding_history"]), MARGIN_FUNDING_SCHEMA
        ).to_pylist()
        assert row["subaccount"] == 85
        assert row["funding_amount"] == Decimal("0.508962")
        assert row["funding_time"].isoformat() == "2026-11-16T15:00:00+00:00"
        assert row["funding_rate"] == pytest.approx(0.000132)

    def test_an_empty_account_is_an_empty_batch(self) -> None:
        assert batch_from_rows([], MARGIN_POSITION_SCHEMA).num_rows == 0
        assert batch_from_rows(flatten_margin_risk({"positions": []}), MARGIN_RISK_SCHEMA).num_rows == 0


class TestDateArguments:
    def test_dates_are_validated_and_normalised(self) -> None:
        from datetime import date

        from vgi_kalshi.account import _date_arg

        assert _date_arg("", "start_date", date(2026, 1, 2)) == "2026-01-02"
        assert _date_arg("2026-11-01", "start_date", date(2026, 1, 2)) == "2026-11-01"
        # A DATE cast to text, or a timestamp, keeps its date.
        assert _date_arg("2026-11-01 00:00:00", "start_date", date(2026, 1, 2)) == "2026-11-01"
        with pytest.raises(ValueError, match="start_date must be a date"):
            _date_arg("Nov 1", "start_date", date(2026, 1, 2))
