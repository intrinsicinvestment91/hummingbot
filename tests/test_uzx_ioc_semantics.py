import asyncio
from decimal import Decimal
from types import SimpleNamespace
import pytest

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.connector.exchange.uzx.uzx_exchange import UzxExchange
from hummingbot.connector.exchange.uzx import uzx_constants as CONSTANTS

class FakeRule:
    min_price_increment = Decimal("0.10")
    min_base_amount_increment = Decimal("0.001")
    min_quote_amount_increment = Decimal("0.01")

class FakeOB:
    def __init__(self, bid, ask):
        self.best_bid_price = Decimal(str(bid))
        self.best_ask_price = Decimal(str(ask))

@pytest.fixture
def ex_base():
    e = UzxExchange.__new__(UzxExchange)
    e._order_book_tracker = SimpleNamespace(order_books={"BTC-USDT": FakeOB(100, 101)})
    e._trading_rules = {"BTC-USDT": FakeRule()}
    e._time_synchronizer = SimpleNamespace(time=lambda: 0.0)
    e.logger = lambda: SimpleNamespace(info=lambda *a, **k: None, debug=lambda *a, **k: None,
                                       warning=lambda *a, **k: None, error=lambda *a, **k: None, setLevel=lambda *a, **k: None)
    e._enable_rounding = False
    e._market_trade_ccy = "quote"
    e._market_ref_price = "bbo"
    e._log_json = False
    e._run_id = "testrun"
    e._order_traces = {}
    async def _last(_): return Decimal("99")
    e._get_last_traded_price = _last
    return e

@pytest.mark.asyncio
async def test_ioc_transform_native_tif(ex_base, monkeypatch):
    e = ex_base
    e._enable_ioc_cap = True
    e._ioc_slippage_bps = 25
    e._ioc_use_tif = True
    e._ioc_cancel_delay_ms = 5
    setattr(CONSTANTS, "TIME_IN_FORCE_IOC", "IOC")
    captured = {}
    async def _fake_post(path_url=None, data=None, is_auth_required=None):
        captured.update(data or {}); return {"data": {"order_id": "123"}}
    e._api_post = _fake_post
    await e._place_order("coid1","BTC-USDT",Decimal("1.0"),TradeType.BUY,OrderType.MARKET,Decimal("NaN"))
    from decimal import Decimal as D
    assert "price" in captured and D(captured["price"]) == D("101.2525")
    assert captured.get("time_in_force") == CONSTANTS.TIME_IN_FORCE_IOC
    assert captured["order_type"] == CONSTANTS.ORDER_TYPE[OrderType.LIMIT]

@pytest.mark.asyncio
async def test_ioc_transform_simulated_cancel_called(ex_base, monkeypatch):
    e = ex_base
    e._enable_ioc_cap = True
    e._ioc_slippage_bps = 25
    e._ioc_use_tif = False
    e._ioc_cancel_delay_ms = 5
    called = {"cancel": False}
    async def _fake_post(path_url=None, data=None, is_auth_required=None):
        return {"data": {"order_id": "456"}}
    async def _fake_put(*args, **kwargs):
        called["cancel"] = True; return {"code": 0}
    e._api_post = _fake_post; e._api_put = _fake_put
    await e._place_order("coid2","BTC-USDT",Decimal("1.0"),TradeType.SELL,OrderType.MARKET,Decimal("NaN"))
    await asyncio.sleep(0.02)
    assert called["cancel"] is True

@pytest.mark.asyncio
async def test_market_semantics_quote_notional(ex_base):
    e = ex_base
    e._enable_ioc_cap = False
    e._market_trade_ccy = "quote"
    e._market_ref_price = "bbo"
    captured = {}
    async def _fake_post(path_url=None, data=None, is_auth_required=None):
        captured.update(data or {}); return {"data": {"order_id": "789"}}
    e._api_post = _fake_post
    await e._place_order("coid3","BTC-USDT",Decimal("0.50"),TradeType.BUY,OrderType.MARKET,Decimal("NaN"))
    from decimal import Decimal as D
    assert captured.get("trade_ccy") == 1 and D(captured.get("amount")) == D("50.5")

@pytest.mark.asyncio
async def test_market_semantics_base_qty(ex_base):
    e = ex_base
    e._enable_ioc_cap = False
    e._market_trade_ccy = "base"
    captured = {}
    async def _fake_post(path_url=None, data=None, is_auth_required=None):
        captured.update(data or {}); return {"data": {"order_id": "101"}}
    e._api_post = _fake_post
    await e._place_order("coid4","BTC-USDT",Decimal("0.50"),TradeType.SELL,OrderType.MARKET,Decimal("NaN"))
    from decimal import Decimal as D
    assert captured.get("trade_ccy") == 0 and D(captured.get("amount")) == D("0.50")
