import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.p2pb2b import (
    p2pb2b_constants as CONSTANTS,
    p2pb2b_utils,
    p2pb2b_web_utils as web_utils,
)
from hummingbot.connector.exchange.p2pb2b.p2pb2b_api_order_book_data_source import P2pb2bAPIOrderBookDataSource
from hummingbot.connector.exchange.p2pb2b.p2pb2b_api_user_stream_data_source import P2pb2bAPIUserStreamDataSource
from hummingbot.connector.exchange.p2pb2b.p2pb2b_auth import P2pb2bAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.network_base import NetworkStatus
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class P2pb2bExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 p2pb2b_api_key: str,
                 p2pb2b_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        print(f"P2PB2B: Connector created with trading_pairs: {trading_pairs}")
        self.api_key = p2pb2b_api_key
        self.secret_key = p2pb2b_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_p2pb2b_timestamp = 1.0
        super().__init__(client_config_map)
        print("P2PB2B: __init__ completed")

    @staticmethod
    def p2pb2b_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(p2pb2b_type: str) -> OrderType:
        return OrderType[p2pb2b_type]

    @property
    def authenticator(self):
        return P2pb2bAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "p2pb2b"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_MARKET_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_MARKET_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)
        return pairs_prices

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("1006" in error_description
                                        or "1012" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE) in str(
            status_update_exception
        ) and CONSTANTS.ORDER_NOT_FOUND_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE) in str(
            cancelation_exception
        ) and CONSTANTS.ORDER_NOT_FOUND_MESSAGE in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return P2pb2bAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return P2pb2bAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        order_result = None
        amount_str = f"{amount:f}"
        type_str = P2pb2bExchange.p2pb2b_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        market = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {"market": market,
                      "side": side_str,
                      "amount": amount_str,
                      "type": type_str
                      }
        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            price_str = f"{price:g}"
            api_params["price"] = price_str
        if order_type == OrderType.LIMIT:
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC

        try:
            response = await self._api_post(
                path_url=CONSTANTS.NEW_ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)
            if response.get("success"):
                order_result = response["result"]
                o_id = str(order_result["orderId"])
                transact_time = order_result["timestamp"] * 1e-3
            else:
                raise Exception(f"Error placing order: {response}")
        except IOError as e:
            error_description = str(e)
            is_server_overloaded = ("status is 503" in error_description
                                    and "Unknown error, please check your request or try again later." in error_description)
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        self.logger().info(f"Cancelling order {order_id} for {symbol} with exchange order ID {tracked_order.exchange_order_id}")
        api_params = {
            "market": symbol,
            "orderId": int(tracked_order.exchange_order_id),
        }
        cancel_result = await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True)
        return cancel_result.get("success")

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example list item:
        {
            "name": "ETH_BTC",
            "stock": "ETH",
            "money": "BTC",
            "precision": {
                "money": "6",
                "stock": "4",
                "fee": "4"
            },
            "limits": {
                "min_amount": "0.001",
                "max_amount": "100000",
                "step_size": "0.0001",
                "min_price": "0.00001",
                "max_price": "922327",
                "tick_size": "0.00001",
                "min_total": "0.0001"
            }
        }, ...
        """
        trading_pair_rules = exchange_info_dict.get("result", [])
        retval = []
        for rule in filter(p2pb2b_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("name"))
                limits = rule.get("limits")

                min_order_size = Decimal(limits.get("min_amount"))
                tick_size = limits.get("tick_size")
                step_size = Decimal(limits.get("step_size"))
                min_notional = Decimal(limits.get("min_total"))

                retval.append(
                    TradingRule(trading_pair,
                                min_order_size=min_order_size,
                                min_price_increment=Decimal(tick_size),
                                min_base_amount_increment=Decimal(step_size),
                                min_notional_size=Decimal(min_notional)))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        !Not possible as the user stream is not available within the P2pb2b API
        """
        pass

    async def _update_order_fills_from_trades(self):
        """
        This is intended to be a backup measure to get filled events with trade ID for orders,
        in case P2pb2b's user stream events are not working.
        NOTE: It is not required to copy this functionality in other connectors.
        This is separated from _update_order_status which only updates the order status without producing filled
        events, since P2pb2b's get order endpoint does not return trade IDs.
        The minimum poll interval for order status is 10 seconds.

        NOTE: Backup is necessary here because user event websocket does not exist in the P2pb2b API
        """
        small_interval_last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        small_interval_current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        long_interval_last_tick = self._last_poll_timestamp / self.LONG_POLL_INTERVAL
        long_interval_current_tick = self.current_timestamp / self.LONG_POLL_INTERVAL

        if (long_interval_current_tick > long_interval_last_tick
                or (self.in_flight_orders and small_interval_current_tick > small_interval_last_tick)):
            self._last_trades_poll_p2pb2b_timestamp = self._time_synchronizer.time()
            order_by_exchange_id_map = {}
            for order in self._order_tracker.all_fillable_orders.values():
                order_by_exchange_id_map[order.exchange_order_id] = order

            tasks = []
            trading_pairs = self.trading_pairs
            for trading_pair in trading_pairs:
                data = {
                    "market": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                }
                # if self._last_poll_timestamp > 0:
                #     data["startTime"] = query_time
                # data["endTime"] = self._time_synchronizer.time() * 1e3
                tasks.append(self._api_post(
                    path_url=CONSTANTS.MY_TRADES_PER_MARKET_PATH_URL,
                    data=data,
                    is_auth_required=True))

            self.logger().debug(f"Polling for order fills of {len(tasks)} trading pairs.")
            results = await safe_gather(*tasks, return_exceptions=True)

            for result, trading_pair in zip(results, trading_pairs):
                if isinstance(result, Exception):
                    self.logger().network(
                        f"Error fetching trades update for the order {trading_pair}: {result}.",
                        app_warning_msg=f"Failed to fetch trade update for {trading_pair}."
                    )
                    continue

                trades = result.get("result", {})
                print(f"P2PB2B: trades for {trading_pair} = {trades}")
                for trade in trades:
                    exchange_order_id = str(trade["id"])
                    deal_id = f"{exchange_order_id}-{trade['time']}"  # Synthetic ID
                    if exchange_order_id in order_by_exchange_id_map:
                        # This is a fill for a tracked order
                        tracked_order = order_by_exchange_id_map[exchange_order_id]
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=tracked_order.trade_type,
                            percent_token=trading_pair.split("_")[1],
                            flat_fees=[TokenAmount(amount=Decimal(trade["deal_fee"]), token=trading_pair.split("_")[1])]
                        )
                        trade_update = TradeUpdate(
                            trade_id=deal_id,
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=exchange_order_id,
                            trading_pair=trading_pair,
                            fee=fee,
                            fill_base_amount=Decimal(trade["amount"]),
                            fill_quote_amount=Decimal(trade["deal"]),
                            fill_price=Decimal(trade["price"]),
                            fill_timestamp=trade["time"] * 1e-3,
                        )
                        self._order_tracker.process_trade_update(trade_update)
                    elif self.is_confirmed_new_order_filled_event(deal_id, exchange_order_id, trading_pair):
                        # This is a fill of an order registered in the DB but not tracked any more
                        self._current_trade_fills.add(TradeFillOrderDetails(
                            market=self.display_name,
                            exchange_trade_id=deal_id,
                            symbol=trading_pair))
                        self.trigger_event(
                            MarketEvent.OrderFilled,
                            OrderFilledEvent(
                                timestamp=float(trade["deal_time"]) * 1e-3,
                                order_id=self._exchange_order_ids.get(exchange_order_id, None),
                                trading_pair=trading_pair,
                                trade_type=TradeType.BUY if trade["side"] == "buy" else TradeType.SELL,
                                order_type=OrderType.LIMIT_MAKER if trade["role"] == "maker" else OrderType.LIMIT,
                                price=Decimal(trade["price"]),
                                amount=Decimal(trade["amount"]),
                                trade_fee=DeductedFromReturnsTradeFee(
                                    flat_fees=[
                                        TokenAmount(
                                            amount=Decimal(trade["deal_fee"]),
                                            token=trading_pair.split("_")[1]
                                        )
                                    ]
                                ),
                                exchange_trade_id=deal_id
                            ))
                        self.logger().info(f"Recreating missing trade in TradeFill: {trade}")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            exchange_order_id = int(order.exchange_order_id)
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            quote = trading_pair.split("_")[1]
            all_fills_response = await self._api_post(
                path_url=CONSTANTS.MY_TRADES_PER_ORDER_PATH_URL,
                data={
                    "market": trading_pair,
                    "orderId": exchange_order_id
                },
                is_auth_required=True,
                limit_id=CONSTANTS.MY_TRADES_PER_ORDER_PATH_URL)
            if all_fills_response.get("success"):
                all_fills_result = all_fills_response.get("result", {})
            else:
                raise Exception(f"Error fetching trades: {all_fills_response}")
            for trade in all_fills_result.get("records", []):
                exchange_order_id = str(trade["dealOrderId"])
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=quote,
                    flat_fees=[TokenAmount(amount=Decimal(trade["fee"]), token=quote)]
                )
                trade_update = TradeUpdate(
                    trade_id=str(trade["id"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(trade["amount"]),
                    fill_quote_amount=Decimal(trade["deal"]),
                    fill_price=Decimal(trade["price"]),
                    fill_timestamp=trade["time"] * 1e-3,
                )
                trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        executed_order_response = await self._api_post(
            path_url=CONSTANTS.MY_TRADES_PER_ORDER_PATH_URL,
            data={
                "market": trading_pair,
                "orderId": tracked_order.exchange_order_id},
            is_auth_required=True)

        executed_orders = executed_order_response.get("result", [{}]).get("records", [])
        self.logger().info(f"P2PB2B: executed_orders = {executed_orders}")
        if executed_order_response.get("success") and tracked_order.exchange_order_id in [executed_orders[i]["id"] for i in range(len(executed_orders))]:
            orders = [executed_orders[i] for i in range(len(executed_orders)) if executed_orders[i]["role"] == tracked_order.order_type.value]
            self.logger().info(f"P2PB2B: orders = {orders}")
            value_to_fill = orders[0]["amount"] * orders[0]["price"]
            total_deals = 0
            for order in orders:
                total_deals += order["deal"]
            if total_deals >= value_to_fill:
                new_state = CONSTANTS.ORDER_STATE["FILLED"]
            else:
                new_state = CONSTANTS.ORDER_STATE["PARTIALLY_FILLED"]
        else:
            new_state = CONSTANTS.ORDER_STATE["NEW"]

        latest_timestamp = self._time_synchronizer.time()

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=latest_timestamp * 1e-3,
            new_state=new_state,
        )

        return order_update

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_post(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True)

        balances = account_info["result"]
        for asset, info in balances.items():
            free_balance = Decimal(info["available"])
            total_balance = Decimal(info["available"]) + Decimal(info["freeze"])
            self._account_available_balances[asset] = free_balance
            self._account_balances[asset] = total_balance
            remote_asset_names.add(asset)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        #! Manual mapping for GNEISS-USDT - need to delete this once the issue is fixed
        # mapping["GNEISS_USDT"] = "GNEISS-USDT"
        # Use "result" key like _format_trading_rules does, not "symbols"
        for symbol_data in filter(p2pb2b_utils.is_exchange_information_valid, exchange_info.get("result", [])):
            # Use P2PB2B field names: "name", "stock", "money"
            mapping[symbol_data["name"]] = combine_to_hb_trading_pair(base=symbol_data["stock"],
                                                                      quote=symbol_data["money"])
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "market": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        resp_json = await self._api_get(
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params=params
        )

        return float(resp_json["last"])

    async def check_network(self) -> NetworkStatus:
        print("P2PB2B: check_network() called")
        try:
            print(f"P2PB2B: Making network check request to {self.check_network_request_path}")
            await self._make_network_check_request()
            print("P2PB2B: Network check successful - returning CONNECTED")
            return NetworkStatus.CONNECTED
        except asyncio.CancelledError:
            print("P2PB2B: Network check cancelled")
            raise
        except Exception as e:
            print(f"P2PB2B: Network check failed with error: {e}")
            import traceback
            traceback.print_exc()
            return NetworkStatus.NOT_CONNECTED

    @property
    def ready(self) -> bool:
        print(f"P2PB2B: ready property called")
        result = super().ready
        print(f"P2PB2B: ready result = {result}")
        if not result:
            status = self.status_dict  # This should trigger your debug log
            print(f"P2PB2B: status breakdown = {status}")
        return result

    # @property
    # def status_dict(self) -> Dict[str, bool]:
    #     print(f"P2PB2B: Checking status components...")

    #     # Check each component individually
    #     try:
    #         symbols_ready = self.trading_pair_symbol_map_ready()
    #         print(f"P2PB2B: symbols_mapping_initialized = {symbols_ready}")
    #     except Exception as e:
    #         print(f"P2PB2B: symbols_mapping check failed: {e}")
    #         symbols_ready = False

    #     try:
    #         order_books_ready = self.order_book_tracker.ready
    #         print(f"P2PB2B: order_books_initialized = {order_books_ready}")
    #         print(f"P2PB2B: order_book_tracker has {len(self.order_book_tracker._order_books)} order books")
    #     except Exception as e:
    #         print(f"P2PB2B: order_books check failed: {e}")
    #         order_books_ready = False

    #     try:
    #         account_balance_ready = not self.is_trading_required or len(self._account_balances) > 0
    #         print(f"P2PB2B: account_balance = {account_balance_ready}")
    #         print(f"P2PB2B: is_trading_required = {self.is_trading_required}")
    #         print(f"P2PB2B: _account_balances length = {len(self._account_balances)}")
    #     except Exception as e:
    #         print(f"P2PB2B: account_balance check failed: {e}")
    #         account_balance_ready = False

    #     try:
    #         trading_rules_ready = len(self._trading_rules) > 0 if self.is_trading_required else True
    #         print(f"P2PB2B: trading_rule_initialized = {trading_rules_ready}")
    #         print(f"P2PB2B: _trading_rules length = {len(self._trading_rules)}")
    #     except Exception as e:
    #         print(f"P2PB2B: trading_rules check failed: {e}")
    #         trading_rules_ready = False

    #     try:
    #         user_stream_ready = self._is_user_stream_initialized()
    #         print(f"P2PB2B: user_stream_initialized = {user_stream_ready}")
    #     except Exception as e:
    #         print(f"P2PB2B: user_stream check failed: {e}")
    #         user_stream_ready = False

    #     status = {
    #         "symbols_mapping_initialized": symbols_ready,
    #         "order_books_initialized": order_books_ready,
    #         "account_balance": account_balance_ready,
    #         "trading_rule_initialized": trading_rules_ready,
    #         "user_stream_initialized": user_stream_ready,
    #     }
    #     print(f"P2PB2B: Final status = {status}")
    #     return status

    # async def start_network(self):
    #     print(f"P2PB2B: START_NETWORK CALLED!")
    #     await super().start_network()
    #     print(f"P2PB2B: START_NETWORK COMPLETED!")
