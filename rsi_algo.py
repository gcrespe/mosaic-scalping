import time
import pandas as pd
import pytz
from datetime import timedelta
import logging
from alpaca_trade_api.rest import TimeFrame

logger = logging.getLogger(__name__)

class RSIAlgo:
    def __init__(self, api, symbol, lot, rsi_period=2, rsi_low=25, rsi_high=75, max_hold_days=30):
        self._api = api
        self._symbol = symbol
        self._lot = lot
        self._rsi_period = rsi_period
        self._rsi_low = rsi_low
        self._rsi_high = rsi_high
        self._max_hold_days = max_hold_days
        self._bars = []
        self._l = logger.getChild(self._symbol)

        # Get current time in NY timezone
        now = pd.Timestamp.now(tz='America/New_York').floor('1min')
        market_open = now.replace(hour=9, minute=30)
        today = now.strftime('%Y-%m-%d')
        
        while True:
            try:
                # Get historical data for the symbol
                data = api.get_bars(
                    symbol, 
                    TimeFrame.Minute, 
                    start=today,
                    limit=1000,
                    adjustment='raw'
                ).df
                
                if not isinstance(data.index, pd.DatetimeIndex):
                    data.index = pd.to_datetime(data.index)

                if data.index.tz is None:
                    data.index = data.index.tz_localize('America/New_York')

                self._bars = data[data.index >= market_open]
                break

            except Exception as e:
                self._l.warning(f"Error getting bars: {e}")
                time.sleep(1)
                continue

        self._init_state()

    def _init_state(self):
        symbol = self._symbol
        order = [o for o in self._api.list_orders() if o.symbol == symbol]
        position = [p for p in self._api.list_positions() if p.symbol == symbol]
        self._order = order[0] if order else None
        self._position = position[0] if position else None
        if self._position:
            if not self._order:
                self._state = 'TO_SELL'
            else:
                self._state = 'SELL_SUBMITTED'
        else:
            self._state = 'TO_BUY'

    def _calculate_rsi(self):
        """Calculate the RSI using the stored bars."""
        closes = self._bars.close.values
        if len(closes) < self._rsi_period + 1:
            return None  # Not enough data

        deltas = pd.Series(closes).diff(1).dropna()
        gains = deltas.where(deltas > 0, 0.0)
        losses = -deltas.where(deltas < 0, 0.0)
        avg_gain = gains.rolling(self._rsi_period).mean().iloc[-1]
        avg_loss = losses.rolling(self._rsi_period).mean().iloc[-1]

        if avg_loss == 0:
            return 100  # Maximum RSI
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _now(self):
        return pd.Timestamp.now(tz='America/New_York')

    def _out_of_market(self):
        return self._now().time() >= pd.Timestamp('15:55').time()

    def on_bar(self, bar):
        """Update with a new bar and check for signals."""
        # Add new bar
        new_bar = pd.DataFrame({
            'open': [bar.open],
            'high': [bar.high],
            'low': [bar.low],
            'close': [bar.close],
            'volume': [bar.volume],
        }, index=[pd.Timestamp(bar.timestamp, tz=pytz.UTC)])
        self._bars = pd.concat([self._bars, new_bar])

        if len(self._bars) < self._rsi_period + 1:
            return  # Not enough data to calculate RSI

        if self._out_of_market():
            return  # Avoid entering or exiting the market late in the day

        rsi_value = self._calculate_rsi()
        self._l.info(f"RSI Value: {rsi_value}")

        # Entry condition: RSI < rsi_low and no open trades
        if self._state == 'TO_BUY' and rsi_value is not None and rsi_value < self._rsi_low:
            self._submit_buy()

        # Exit conditions
        if self._state == 'TO_SELL' and self._position:
            # Exit conditions
            position_age = self._now() - pd.Timestamp(self._position.entry_time)
            if (rsi_value > self._rsi_high or
                self._now().weekday() == 4 or  # Friday
                position_age > timedelta(days=self._max_hold_days)):
                self._submit_sell()

    def _submit_buy(self):
        trade = self._api.get_latest_trade(self._symbol)
        amount = int(self._lot / trade.price)
        try:
            order = self._api.submit_order(
                symbol=self._symbol,
                side='buy',
                type='market',
                qty=amount,
                time_in_force='day',
            )
            self._order = order
            self._transition('BUY_SUBMITTED')
            self._l.info(f"Submitted BUY order: {order}")
        except Exception as e:
            self._l.error(f"Error submitting BUY order: {e}")

    def _submit_sell(self):
        try:
            order = self._api.submit_order(
                symbol=self._symbol,
                side='sell',
                qty=self._position.qty,
                type='market',
                time_in_force='day',
            )
            self._order = order
            self._transition('SELL_SUBMITTED')
            self._l.info(f"Submitted SELL order: {order}")
        except Exception as e:
            self._l.error(f"Error submitting SELL order: {e}")

    def _transition(self, new_state):
        self._l.info(f"Transitioning from {self._state} to {new_state}")
        self._state = new_state
