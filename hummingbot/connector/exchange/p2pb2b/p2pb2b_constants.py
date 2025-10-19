import sys

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "p2pb2b_main"
REST_URLS = {
    "p2pb2b_main": "https://api.p2pb2b.com/api",
    "p2pb2b_stream": "wss://apiws.p2pb2b.com/",
}
WSS_URL = "wss://apiws.p2pb2b.com/"
WS_HEARTBEAT_TIME_INTERVAL = 80

API_VERSION = "/v2"

# Public API endpoints
TICKER_PRICE_CHANGE_PATH_URL = "/ticker"
TICKER_BOOK_PATH_URL = "/markets"
PRICES_PATH_URL = "/ticker/price"
EXCHANGE_MARKET_INFO_PATH_URL = "/markets"
PING_PATH_URL = "/ping"
SNAPSHOT_PATH_URL = "/book"
MAX_ORDER_ID_LEN = 20
HBOT_ORDER_ID_PREFIX = ""

# Private API endpoints
ACCOUNTS_PATH_URL = "/account/balances"
MY_TRADES_PER_ORDER_PATH_URL = "/account/order"
MY_TRADES_PER_MARKET_PATH_URL = "/account/executed_history"
MY_UNEXECUTED_ORDERS_PATH_URL = "/orders"
NEW_ORDER_PATH_URL = "/order/new"
CANCEL_ORDER_PATH_URL = "/order/cancel"

TIME_IN_FORCE_GTC = "GTC"  # Good till cancelled
TIME_IN_FORCE_IOC = "IOC"  # Immediate or cancel
TIME_IN_FORCE_FOK = "FOK"  # Fill or kill

# Rate Limit Type
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
ORDERS_24HR = "ORDERS_24HR"
RAW_REQUESTS = "RAW_REQUESTS"

SIDE_SELL = "sell"
SIDE_BUY = "buy"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUESTS = 10
NO_LIMIT = sys.maxsize

# Websocket event types
DIFF_EVENT_TYPE = "depth.update"
TRADE_EVENT_TYPE = "deals.update"

# Order States
ORDER_STATE = {
    "PENDING": OrderState.PENDING_CREATE,
    "NEW": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "PENDING_CANCEL": OrderState.OPEN,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.FAILED,
    "EXPIRED_IN_MATCH": OrderState.FAILED,
}

RATE_LIMITS = [
    # Public endpoints
    RateLimit(limit_id=PING_PATH_URL, limit=NO_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=NO_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_PATH_URL, limit=NO_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id=EXCHANGE_MARKET_INFO_PATH_URL, limit=NO_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TICKER_BOOK_PATH_URL, limit=NO_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id=PRICES_PATH_URL, limit=NO_LIMIT, time_interval=ONE_MINUTE),
    # Private endpoints
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=MAX_REQUESTS, time_interval=ONE_MINUTE),
    RateLimit(limit_id=MY_TRADES_PER_ORDER_PATH_URL, limit=MAX_REQUESTS, time_interval=ONE_MINUTE),
    RateLimit(limit_id=MY_TRADES_PER_MARKET_PATH_URL, limit=MAX_REQUESTS, time_interval=ONE_MINUTE),
    RateLimit(limit_id=NEW_ORDER_PATH_URL, limit=MAX_REQUESTS, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=MAX_REQUESTS, time_interval=ONE_MINUTE),
]

ORDER_NOT_FOUND_ERROR_CODE = "2030"
ORDER_NOT_FOUND_MESSAGE = "Order not found"
