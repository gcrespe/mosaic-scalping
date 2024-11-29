import alpaca_trade_api as alpaca
import asyncio
import pandas as pd
import pytz
from flask import Flask
import logging
import time
import argparse
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import TimeFrame

logger = logging.getLogger()

ALPACA_API_KEY = "PK0AL2LH58FK5WFZFT1P"
ALPACA_SECRET_KEY = "QlikwJZnFjc4WsrtFSy4zsYkHief0JVxgn2vicWn"

def setup_logging():
    fmt = (
        '\n%(asctime)s | %(name)s | %(levelname)s\n'
        '    Symbol: %(symbol)s\n'
        '    Event: %(event)s\n'
        '    Details: %(message)s\n'
    )
    
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler('console.log'),
            logging.StreamHandler()  # Also print to console
        ]
    )
    return logging.getLogger()


class ScalpAlgo:
    def __init__(self, api, symbol, lot):
        self._api = api
        self._symbol = symbol
        self._lot = lot
        self._bars = []
        self._l = logger.getChild(self._symbol)

        # Get current time in NY timezone
        now = pd.Timestamp.now(tz='America/New_York').floor('1min')
        market_open = now.replace(hour=9, minute=30)
        today = now.strftime('%Y-%m-%d')
        
        while True:
            try:
                # Get the bars data
                data = api.get_bars(
                    symbol, 
                    TimeFrame.Minute, 
                    start=today,
                    limit=1000,
                    adjustment='raw'
                ).df
                
                # Convert index to datetime if it's not already
                if not isinstance(data.index, pd.DatetimeIndex):
                    data.index = pd.to_datetime(data.index)
                
                # Localize the index to NY timezone
                if data.index.tz is None:
                    data.index = data.index.tz_localize('America/New_York')
                
                # Filter for market hours data
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
        position = [p for p in self._api.list_positions()
                    if p.symbol == symbol]
        self._order = order[0] if len(order) > 0 else None
        self._position = position[0] if len(position) > 0 else None
        if self._position is not None:
            if self._order is None:
                self._state = 'TO_SELL'
            else:
                self._state = 'SELL_SUBMITTED'
                if self._order.side != 'sell':
                    self._l.warn(
                        f'state {self._state} mismatch order {self._order}')
        else:
            if self._order is None:
                self._state = 'TO_BUY'
            else:
                self._state = 'BUY_SUBMITTED'
                if self._order.side != 'buy':
                    self._l.warn(
                        f'state {self._state} mismatch order {self._order}')

    def _now(self):
        return pd.Timestamp.now(tz='America/New_York')

    def _outofmarket(self):
        return self._now().time() >= pd.Timestamp('15:55').time()

    def checkup(self, position):
        # self._l.info('periodic task')

        now = self._now()
        order = self._order
        if (order is not None and
            order.side == 'buy' and now -
                order.submitted_at.tz_convert(tz='America/New_York') > pd.Timedelta('2 min')):
            last_price = self._api.get_latest_trade(self._symbol).price
            self._l.info(
                f'canceling missed buy order {order.id} at {order.limit_price} '
                f'(current price = {last_price})')
            self._cancel_order()

        if self._position is not None and self._outofmarket():
            self._submit_sell(bailout=True)

    def _cancel_order(self):
        if self._order is not None:
            self._api.cancel_order(self._order.id)

    def _calc_buy_signal(self):
        mavg = self._bars.rolling(20).mean().close.values
        closes = self._bars.close.values
        prev_close = closes[-2]
        current_close = closes[-1]
        prev_ma = mavg[-2]
        current_ma = mavg[-1]

        if prev_close < prev_ma and current_close > current_ma:
            self._l.info(
                f'BUY SIGNAL | '
                f'Previous Close: ${prev_close:.2f} < Previous MA: ${prev_ma:.2f} | '
                f'Current Close: ${current_close:.2f} > Current MA: ${current_ma:.2f}'
            )
            return True
        else:
            self._l.info(
                f'NO SIGNAL | '
                f'Previous Close: ${prev_close:.2f}, Current Close: ${current_close:.2f} | '
                f'Previous MA: ${prev_ma:.2f}, Current MA: ${current_ma:.2f}'
            )
            return False

    def on_bar(self, bar):
        # Create new bar DataFrame
        new_bar = pd.DataFrame({
            'open': [bar.open],
            'high': [bar.high],
            'low': [bar.low],
            'close': [bar.close],
            'volume': [bar.volume],
        }, index=[pd.Timestamp(bar.timestamp, tz=pytz.UTC)])

        # Concatenate the new bar with existing bars
        self._bars = pd.concat([self._bars, new_bar])

        message = (
            f"Symbol: {self._symbol} | "
            f"Event: BAR_UPDATE | "
            f"Close: {bar.close:.2f} | "
            f"Open: {bar.open:.2f} | "
            f"High: {bar.high:.2f} | "
            f"Low: {bar.low:.2f} | "
            f"Volume: {bar.volume}"
        )
        self._l.info(message)
        
        if len(self._bars) < 21:
            return
        if self._outofmarket():
            return
        if self._state == 'TO_BUY':
            signal = self._calc_buy_signal()
            if signal:
                self._submit_buy()

    def on_order_update(self, event, order):
        message = (
            f"Symbol: {self._symbol} | "
            f"Event: ORDER_{event.upper()} | "
            f"Order ID: {order.get('id', 'Unknown')} | "
            f"Status: {order.get('status', 'Unknown')} | "
            f"Side: {order.get('side', 'Unknown')} | "
            f"Type: {order.get('type', 'Unknown')} | "
            f"Qty: {order.get('qty', 'Unknown')} | "
            f"Filled: {order.get('filled_qty', '0')} | "
            f"Price: ${float(order.get('limit_price', 0)):.2f}"
        )
        self._l.info(message)

        if event == 'fill':
            self._order = None
            if self._state == 'BUY_SUBMITTED':
                self._position = self._api.get_position(self._symbol)
                self._transition('TO_SELL')
                self._submit_sell()
                return
            elif self._state == 'SELL_SUBMITTED':
                self._position = None
                self._transition('TO_BUY')
                return
        elif event == 'partial_fill':
            self._position = self._api.get_position(self._symbol)
            self._order = self._api.get_order(order['id'])
            return
        elif event in ('canceled', 'rejected'):
            if event == 'rejected':
                self._l.warn(f'order rejected: current order = {self._order}')
            self._order = None
            if self._state == 'BUY_SUBMITTED':
                if self._position is not None:
                    self._transition('TO_SELL')
                    self._submit_sell()
                else:
                    self._transition('TO_BUY')
            elif self._state == 'SELL_SUBMITTED':
                self._transition('TO_SELL')
                self._submit_sell(bailout=True)
            else:
                self._l.warn(f'unexpected state for {event}: {self._state}')

    def _submit_buy(self):
        trade = self._api.get_latest_trade(self._symbol)
        amount = int(self._lot / trade.price)
        
        try:
            order = self._api.submit_order(
                symbol=self._symbol,
                side='buy',
                type='limit',
                qty=amount,
                time_in_force='day',
                limit_price=trade.price,
            )
            
            self._l.info('', extra={
                'symbol': self._symbol,
                'event': 'SUBMIT_BUY',
                'message': (
                    f"Price: ${trade.price:.2f}\n"
                    f"    Quantity: {amount}\n"
                    f"    Total Value: ${trade.price * amount:.2f}\n"
                    f"    Order ID: {order.id}"
                )
            })
            
            self._order = order
            self._transition('BUY_SUBMITTED')
            
        except Exception as e:
            self._l.error({
                'symbol': self._symbol,
                'event': 'BUY_ERROR',
                'message': e
            })
            self._transition('TO_BUY')

    def _submit_sell(self, bailout=False):
        params = dict(
            symbol=self._symbol,
            side='sell',
            qty=self._position.qty,
            time_in_force='day',
        )
        if bailout:
            params['type'] = 'market'
        else:
            current_price = float(
                self._api.get_latest_trade(
                    self._symbol).price)
            cost_basis = float(self._position.avg_entry_price)
            limit_price = max(cost_basis + 0.01, current_price)
            params.update(dict(
                type='limit',
                limit_price=limit_price,
            ))
        try:
            order = self._api.submit_order(**params)
        except Exception as e:
            self._l.error({
                'symbol': self._symbol,
                'event': 'BUY_ERROR',
                'message': e
            })
            self._transition('TO_SELL')
            return

        self._order = order
        self._l.info(f'submitted sell {order}')
        self._transition('SELL_SUBMITTED')

    def _transition(self, new_state):
        self._l.info(f'transition from {self._state} to {new_state}')
        self._state = new_state


def main():
    stream = Stream(ALPACA_API_KEY,
                    ALPACA_SECRET_KEY,
                    base_url=URL('https://paper-api.alpaca.markets'),
                    data_feed='iex')  # <- replace to sip for PRO subscription
    api = alpaca.REST(key_id=ALPACA_API_KEY,
                    secret_key=ALPACA_SECRET_KEY,
                    base_url="https://paper-api.alpaca.markets")

    fleet = {}
    symbols = ['AAPL', 'TLSA', 'QQQ']
    for symbol in symbols:
        algo = ScalpAlgo(api, symbol, lot=2000)
        fleet[symbol] = algo

    async def on_bars(data):
        if data.symbol in fleet:
            fleet[data.symbol].on_bar(data)

    for symbol in symbols:
        stream.subscribe_bars(on_bars, symbol)

    async def on_trade_updates(data):
        event = data.event
        order = data.order
        symbol = order.get('symbol', 'Unknown')
        order_id = order.get('id', 'Unknown')
        side = order.get('side', 'Unknown')
        status = order.get('status', 'Unknown')
        filled_qty = order.get('filled_qty', '0')
        qty = order.get('qty', 'Unknown')
        filled_avg_price = order.get('filled_avg_price', '0.00')
        timestamp = order.get('filled_at', 'Unknown')

        # Create a formatted log message
        message = (
            f"Trade Update | "
            f"Event: {event.upper()} | "
            f"Symbol: {symbol} | "
            f"Order ID: {order_id} | "
            f"Side: {side} | "
            f"Status: {status} | "
            f"Quantity: {qty} | "
            f"Filled Quantity: {filled_qty} | "
            f"Filled Average Price: ${float(filled_avg_price):.2f} | "
            f"Timestamp: {timestamp}"
        )

        logger.info(message)
        
        symbol = data.order['symbol']
        if symbol in fleet:
            fleet[symbol].on_order_update(data.event, data.order)

    stream.subscribe_trade_updates(on_trade_updates)

    async def periodic():
        while True:
            if not api.get_clock().is_open:
                logger.info("Market is closed. Program is idle.")
                await asyncio.sleep(300)  # Sleep for 5 minutes while idle
                continue
            await asyncio.sleep(30)
            positions = api.list_positions()
            for symbol, algo in fleet.items():
                pos = [p for p in positions if p.symbol == symbol]
                algo.checkup(pos[0] if len(pos) > 0 else None)

    # Create and set the event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(asyncio.gather(
            stream._run_forever(),
            periodic(),
        ))
    finally:
        loop.close()

def create_app():
    app = Flask(__name__)
    fmt = (
        '%(asctime)s | %(name)s | %(levelname)s | '
        '%(message)s'
    )
    
    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler('console.log'),
            logging.StreamHandler()  # This will print to console as well
        ]
    )
    main()
    return app

app = create_app()
app.run(host="0.0.0.0", port=3001, debug=True)
    