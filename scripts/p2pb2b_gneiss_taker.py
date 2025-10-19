import logging
import random

from hummingbot.core.data_type.common import OrderType
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy.strategy_py_base import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)


class SimpleOrder(ScriptStrategyBase):
    """
    This script places orders on the P2PB2B exchange for the GNEISS token.
    """

    # Key Parameters
    exchange = "p2pb2b"
    base = "GNEISS"
    quote = "USDT"

    # Other Parameters
    markets = {
        exchange: {f"{base}-{quote}"}
    }

    trade_side = 0  # 0 for buy, 1 for sell

    ONE_HOUR = 3600  # 1 hour in seconds
    TRADE_AMOUNT_USD_MINIMUM = 1250  # 1250 USDT worth of trades per hour
    TRADE_AMOUNT_USD_MAXIMUM = 1500  # 1500 USDT worth of trades per hour
    TRADE_FREQUENCY_MINIMUM = 60  # 60 trades per hour
    time_tracker = 0  # Tracks how much time has passed in seconds - limited to one hour at a time
    trade_times = []  # Determination of when within the hour to trade

    def init_future_trades(self):
        # create a bucket range to increment by
        bucket_range = self.ONE_HOUR // self.TRADE_FREQUENCY_MINIMUM
        for i in range(self.TRADE_FREQUENCY_MINIMUM):
            # append a random trade time within the bucket range
            self.trade_times.append(
                int(random.randint(i * bucket_range, (i + 1) * bucket_range))
            )
        self.trade_times = sorted(self.trade_times)
        self.log_with_clock(logging.INFO, f"Trade times: {self.trade_times}")

    def should_trade(self):
        """
        Returns True if the current time tracker is within 1 second of any of the trade times
        """
        return any(abs(self.time_tracker - t) <= 1 for t in self.trade_times)

    def place_order(self, amount):
        # places order
        if self.trade_side:
            self.sell(
                connector_name=self.exchange,
                trading_pair=f"{self.base}-{self.quote}",
                amount=amount,
                order_type=OrderType.MARKET
            )
        else:
            self.buy(
                connector_name=self.exchange,
                trading_pair=f"{self.base}-{self.quote}",
                amount=amount,
                order_type=OrderType.MARKET
            )
        # remove the trade time that was just used
        self.trade_times.pop(0)

    def on_tick(self):
        if self.time_tracker >= self.ONE_HOUR:
            self.time_tracker = 0
            self.init_future_trades()

        if self.should_trade():
            order_amount_usd = round(
                random.uniform(
                    self.TRADE_AMOUNT_USD_MINIMUM / self.TRADE_FREQUENCY_MINIMUM,
                    self.TRADE_AMOUNT_USD_MAXIMUM / self.TRADE_FREQUENCY_MINIMUM
                ), 2)  # Random amount of USDT to trade - lowest it can be is the min hourly trade amount after multiplying by the frequency
            conversion_rate = RateOracle.get_instance().get_pair_rate(f"{self.base}-USDT")
            amount = order_amount_usd / conversion_rate
            price = self.connectors[self.exchange].get_mid_price(f"{self.base}-{self.quote}")

            self.place_order(amount)
            self.trade_side ^= 1  # toggle trade side
            self.log_with_clock(logging.INFO, f"Trade side flipped to: {'sell' if self.trade_side else 'buy'}")
        self.time_tracker += 1

    def did_fill_order(self, event: OrderFilledEvent):
        msg = (f"{event.trade_type.name} {event.amount} of {event.trading_pair} {self.exchange} at {event.price}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        msg = (f"Order {event.order_id} to buy {event.base_asset_amount} of {event.base_asset} is completed.")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        msg = (f"Order {event.order_id} to sell {event.base_asset_amount} of {event.base_asset} is completed.")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        msg = (f"Created BUY order {event.order_id}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        msg = (f"Created SELL order {event.order_id}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
