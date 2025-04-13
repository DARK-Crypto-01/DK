import asyncio
import json
import time
import logging
import websockets
from typing import Callable, Optional, Dict
from exchange_adapter import ExchangeAdapter

class GenericWebSocketConnection:
    def __init__(self, 
                 adapter: ExchangeAdapter,
                 on_price_callback: Callable[[float], None],
                 on_order_callback: Callable[[str, dict], None],
                 instance_range: tuple = (0, 0),
                 ws_manager: Optional[object] = None,
                 is_event_connection: bool = False):
        
        self.adapter = adapter
        self.on_price_callback = on_price_callback
        self.on_order_callback = on_order_callback
        self.logger = logging.getLogger("WSConnection")
        self._connection = None
        self._keep_running = True
        self.instance_range = instance_range
        self.ws_manager = ws_manager
        self._is_reconnect = False
        self.is_event_connection = is_event_connection
        self.currency_pair = None

        if self.is_event_connection:
            self.currency_pair = self.adapter.format_symbol(
                self.adapter.strategy_cfg['currency_pair']
            )
            self.pending_orders_lock = asyncio.Lock()
            self.pending_orders = {}  # {client_order_id: (callback, instance_index)}

    def _get_ws_url(self):
        env = 'testnet' if self.adapter.exchange_cfg['testnet'] else 'mainnet'
        return self.adapter.exchange_cfg['api']['ws_base'][env]

    async def connect(self) -> None:
        retry_config = self.adapter.strategy_cfg['retry_config']['websocket']
        max_retries = retry_config.get('max_retries', 3)
        retry_count = 0
        
        while self._keep_running and retry_count <= max_retries:
            try:
                self._is_reconnect = retry_count > 0
                async with websockets.connect(self._get_ws_url()) as ws:
                    self._connection = ws
                    await self._on_open()
                    async for message in ws:
                        await self._on_message(message)
                    retry_count = 0
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    self.logger.critical("Max WS retries exceeded")
                    break
                await self._handle_reconnect(retry_count, e, retry_config)
            finally:
                await self._on_close()
                self._connection = None

    async def _handle_reconnect(self, retry_count: int, error: Exception, config: dict):
        if config.get('first_retry_immediate') and retry_count == 1:
            delay = 0
        else:
            base = config.get('initial_interval', 0.01)
            multiplier = config.get('multiplier', 2)
            delay = base * (multiplier ** (retry_count - 1))

        self.logger.error(f"Connection error: {str(error)}. Retry {retry_count} in {delay:.2f}s")
        await asyncio.sleep(delay)

    async def _on_open(self) -> None:
        self.logger.info(f"Connected to {self._get_ws_url()}")
        
        if self._is_reconnect and self.is_event_connection:
            await self._handle_reconnect_cleanup()
        
        try:
            if self.is_event_connection:
                await self._subscribe_channel(
                    self.adapter.exchange_cfg['channels']['ticker']['name'],
                    [self.currency_pair]
                )
                await self._subscribe_channel(
                    self.adapter.exchange_cfg['channels']['orders']['name'],
                    [self.currency_pair]
                )
        except Exception as e:
            self.logger.error(f"Subscription failed: {str(e)}")

    async def _subscribe_channel(self, channel: str, payload: list):
        timestamp = int(time.time())
        params = f"channel={channel}&event=subscribe&time={timestamp}"
        
        msg = {
            "time": timestamp,
            "channel": channel,
            "event": "subscribe",
            "payload": payload,
            "auth": self.adapter.create_auth_payload(params)
        }
        await self.async_send_message(msg)

    async def _on_message(self, message: str) -> None:
        self.logger.debug(f"Raw message: {message[:200]}")
        
        try:
            if self.is_event_connection:
                if self._is_ticker_message(message):
                    price = self.adapter.parse_message(message, "ticker")
                    if price is not None:
                        self.on_price_callback(price)

                elif self._is_order_message(message):
                    order_data = self.adapter.parse_message(message, "order")
                    if order_data:
                        await self._process_order_event(order_data)
        except Exception as e:
            self.logger.error(f"Message handling error: {str(e)}")

    def _is_ticker_message(self, message: str) -> bool:
        try:
            data = json.loads(message)
            channel_key = self.adapter.exchange_cfg['message_templates']['subscribe']['channel_key']
            event_key = self.adapter.exchange_cfg['message_templates']['subscribe']['event_key']
            
            expected_channel = self.adapter.exchange_cfg['channels']['ticker']['name']
            expected_event = self.adapter.exchange_cfg['channels']['ticker'].get('event_type')
            
            # Match both channel and event type if configured
            channel_match = data.get(channel_key) == expected_channel
            event_match = (expected_event is None or 
                          data.get(event_key) == expected_event)
            
            return channel_match and event_match
        except:
            return False

    def _is_order_message(self, message: str) -> bool:
        try:
            data = json.loads(message)
            return data.get(
                self.adapter.exchange_cfg['message_templates']['subscribe']['channel_key']
            ) == self.adapter.exchange_cfg['channels']['orders']['name']
        except:
            return False

    async def _process_order_event(self, order_data: dict) -> None:
        if not self.is_event_connection:
            return

        client_order_id = order_data.get('client_order_id')
        status = order_data.get('status')
        symbol = order_data.get('symbol')

        if symbol != self.currency_pair:
            self.logger.debug(f"Ignoring order for {symbol}")
            return

        async with self.pending_orders_lock:
            callback_info = self.pending_orders.pop(client_order_id, None)

        enhanced_event = {
            **order_data,
            'client_order_id': client_order_id,
            'instance_index': callback_info[1] if callback_info else None,
            'filled': order_data.get('filled', 0),
            'amount': order_data.get('amount', 0)
        }

        if callback_info:
            callback, _ = callback_info
            if asyncio.iscoroutinefunction(callback):
                await callback(client_order_id, enhanced_event)
            else:
                callback(client_order_id, enhanced_event)
                
        self.on_order_callback(client_order_id, enhanced_event)

    async def _handle_reconnect_cleanup(self):
        self.logger.info("Performing reconnect cleanup")
        if self.ws_manager:
            await self.ws_manager.clean_instance_states(*self.instance_range)

    async def async_send_message(self, message: dict) -> None:
        try:
            if self._connection and self._connection.open:
                await self._connection.send(json.dumps(message))
            else:
                self.logger.error("Cannot send message - WS not connected")
        except Exception as e:
            self.logger.error(f"Message send failed: {str(e)}")

    async def register_pending_order(self,
                                    client_order_id: str,
                                    callback: Callable,
                                    instance_index: int) -> None:
        if self.is_event_connection:
            async with self.pending_orders_lock:
                self.pending_orders[client_order_id] = (callback, instance_index)

    async def close(self) -> None:
        self._keep_running = False
        if self._connection:
            await self._connection.close()

    async def start(self) -> None:
        self.logger.info("Starting WebSocket connection")
        asyncio.create_task(self.connect())
