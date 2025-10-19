import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.p2pb2b import p2pb2b_constants as CONSTANTS, p2pb2b_web_utils as web_utils
from hummingbot.connector.exchange.p2pb2b.p2pb2b_order_book import P2pb2bOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.p2pb2b.p2pb2b_exchange import P2pb2bExchange


class P2pb2bAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'P2pb2bExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        self.logger().info(f"Fetching order book snapshot for {trading_pair}")
        rest_assistant = await self._api_factory.get_rest_assistant()
        sell_data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_PATH_URL, domain=self._domain),
            params={
            "market": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": "100",
            "side": "sell"
        },
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
        )

        self.logger().info(f"Sell data: {sell_data}")

        buy_data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_PATH_URL, domain=self._domain),
            params={
            "market": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": "100",
            "side": "buy"
        },
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
        )

        self.logger().info(f"Buy data: {buy_data}")

        if all([sell_data.get("success"), buy_data.get("success")]):
            combined_orders = {
                "orders": [*sell_data.get("result", {}).get("orders", []),
                           *buy_data.get("result", {}).get("orders", [])]
            }
            self.logger().info(f"Combined orders: {combined_orders}")
            return combined_orders
        else:
            raise Exception(f"Error fetching order book snapshot for {trading_pair}: {sell_data} {buy_data}")

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            trade_params = []

            depth_request_id = 2
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trade_params.append(symbol)
                depth_payload = {
                    "method": "depth.subscribe",
                    "params": [symbol, 10, "0"],
                    "id": depth_request_id
                }
                subscribe_depth_request: WSJSONRequest = WSJSONRequest(payload=depth_payload)
                await ws.send(subscribe_depth_request)
                depth_request_id += 1

            trade_payload = {
                "method": "deals.subscribe",
                "params": trade_params,
                "id": 1
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)

            await ws.send(subscribe_trade_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL,
                         ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = P2pb2bOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if raw_message.get("method") == self._trade_messages_queue_key:
            params = raw_message["params"]
            symbol = params[0]  # "ETH_BTC"
            trades = params[1]  # List of trade objects

            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

            # P2PB2B sends multiple trades in one message, process each one
            for trade_data in trades:
                trade_message = P2pb2bOrderBook.trade_message_from_exchange(
                    trade_data, {"trading_pair": trading_pair, "symbol": symbol})
                message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if raw_message.get("method") == "depth.update":
            params = raw_message["params"]
            all_records = params[0]

            if not all_records:  # Only process incremental updates
                order_book_data = params[1]
                symbol = params[2]
                timestamp = time.time()

                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                # Generate update_id from current time
                update_id = int(time.time())

                order_book_message: OrderBookMessage = P2pb2bOrderBook.diff_message_from_exchange(
                    order_book_data, timestamp, {"trading_pair": trading_pair, "update_id": update_id})
                message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "method" in event_message:
            method = event_message.get("method")
            if method == self._trade_messages_queue_key:
                channel = self._trade_messages_queue_key
            elif method == self._diff_messages_queue_key:
                channel = self._diff_messages_queue_key
        return channel
