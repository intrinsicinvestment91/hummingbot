from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "ZRX-ETH"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    # P2PB2B specific validation - check if required fields exist
    has_required_fields = all(field in exchange_info for field in ["name", "stock", "money"])
    return has_required_fields


class P2pb2bConfigMap(BaseConnectorConfigMap):
    connector: str = "p2pb2b"
    p2pb2b_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your P2pb2b API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    p2pb2b_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your P2pb2b API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="p2pb2b")


KEYS = P2pb2bConfigMap.model_construct()
