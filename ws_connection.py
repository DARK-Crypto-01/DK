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
        self.logger = logging.getLogger(f"WSConnection.{adapter.exchange_cfg['name']}")
        self._connection = None
        self._keep_running = True
        self.instance_range = instance_range
        self.ws_manager = ws_manager
        self._is_reconnect = False
        self.is_event_connection = is_event_connection
        self.currency_pair = None
        self._connection_attempts = 0
        self._messages_received = 0
        self._messages_processed = 0

        if self.is_event_connection:
            self.currency_pair = self.adapter.format_symbol(
                self.adapter.strategy_cfg['currency_pair']
            )
            self.pending_orders_lock = asyncio.Lock()
            self.pending_orders = {}
            self.logger = self.logger.getChild("Event")
        else:
            self.logger = self.logger.getChild(f"Orders.{instance_range[0]}-{instance_range[1]}")

        self.logger.debug(f"Initialized WebSocket connection for instances {instance_range}")

    def _get_ws_url(self):
        env = 'testnet' if self.adapter.exchange_cfg['testnet'] else 'mainnet'
        url = self.adapter.exchange_cfg['api']['ws_base'][env]
        self.logger.debug(f"Resolved WebSocket URL: {url}")
        return url

    async def connect(self) -> None:
        retry_config = self.adapter.strategy_cfg['retry_config']['websocket']
        max_retries = retry_config.get('max_retries', 3)
        retry_count = 0
        
        self.logger.info(f"Starting connection process with {max_retries} max retries")
        
        while self._keep_running and retry_count <= max_retries:
            try:
                self._connection_attempts += 1
                self._is_reconnect = retry_count > 0
                connect_start = time.monotonic()
                
                self.logger.debug(f"Attempting connection (attempt {self._connection_attempts})")
                async with websockets.connect(self._get_ws_url()) as ws:
                    self._connection = ws
                    connect_duration = time.monotonic() - connect_start
                    self.logger.info(f"Connected successfully in {connect_duration:.2f}s")
                    
                    await self._on_open()
                    
                    async for message in ws:
                        self._messages_received += 1
                        await self._on_message(message)
                        
                    retry_count = 0
                    self.logger.warning("WebSocket connection closed normally by server")
                    
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Connection error: {str(e)}", exc_info=self.logger.isEnabledFor(logging.DEBUG))
                
                if retry_count > max_retries:
                    self.logger.critical("Max connection retries exceeded. Permanent failure.")
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

        self.logger.warning(f"Reconnection attempt {retry_count} in {delay:.2f}s. Error: {str(error)}")
        await asyncio.sleep(delay)

    async def _on_open(self) -> None:
        self.logger.info("WebSocket connection established")
        
        if self._is_reconnect and self.is_event_connection:
            self.logger.debug("Starting reconnect cleanup process")
            await self._handle_reconnect_cleanup()
        
        try:
            if self.is_event_connection:
                self.logger.debug("Subscribing to market data channels")
                await self._subscribe_channel(
                    self.adapter.exchange_cfg['channels']['ticker']['name'],
                    [self.currency_pair]
                )
                await self._subscribe_channel(
                    self.adapter.exchange_cfg['channels']['orders']['name'],
                    [self.currency_pair]
                )
        except Exception as e:
            self.logger.error(f"Subscription failed: {str(e)}", exc_info=self.logger.isEnabledFor(logging.DEBUG))

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
        
        self.logger.debug(f"Subscribing to channel {channel} with payload {payload}")
        await self.async_send_message(msg)

    async def _on_message(self, message: str) -> None:
        self.logger.debug(f"Received message (length: {len(message)} bytes)")
        
        try:
            if self.is_event_connection:
                if self._is_ticker_message(message):
                    self.logger.debug("Processing ticker message")
                    price = self.adapter.parse_message(message, "ticker")
                    if price is not None:
                        self.logger.debug(f"Valid price update: {price}")
                        self.on_price_callback(price)
                        self._messages_processed += 1
                    else:
                        self.logger.debug("Ignored invalid ticker message")

                elif self._is_order_message(message):
                    self.logger.debug("Processing order message")
                    order_data = self.adapter.parse_message(message, "order")
                    if order_data:
                        self.logger.info(f"Received order event: {order_data}")
                        await self._process_order_event(order_data)
                        self._messages_processed += 1
                    else:
                        self.logger.debug("Ignored unprocessable order message")
        except Exception as e:
            self.logger.error(f"Message handling failed: {str(e)}", exc_info=self.logger.isEnabledFor(logging.DEBUG))

    def _is_ticker_message(self, message: str) -> bool:
        try:
            data = json.loads(message)
            channel_key = self.adapter.exchange_cfg['message_templates']['subscribe']['channel_key']
            event_key = self.adapter.exchange_cfg['message_templates']['subscribe']['event_key']
            
            expected_channel = self.adapter.exchange_cfg['channels']['ticker']['name']
            expected_event = self.adapter.exchange_cfg['channels']['ticker'].get('event_type')
            
            channel_match = data.get(channel_key) == expected_channel
            event_match = (expected_event is None or 
                          data.get(event_key) == expected_event)
            
            if channel_match and event_match:
                self.logger.debug(f"Identified ticker message: {data.get(channel_key)}/{data.get(event_key)}")
                return True
            return False
        except Exception as e:
            self.logger.debug(f"Ticker message validation failed: {str(e)}")
            return False

    def _is_order_message(self, message: str) -> bool:
        try:
            data = json.loads(message)
            channel_key = self.adapter.exchange_cfg['message_templates']['subscribe']['channel_key']
            channel_name = self.adapter.exchange_cfg['channels']['orders']['name']
            
            if data.get(channel_key) == channel_name:
                self.logger.debug(f"Identified order message: {data.get(channel_key)}")
                return True
            return False
        except Exception as e:
            self.logger.debug(f"Order message validation failed: {str(e)}")
            return False

    async def _process_order_event(self, order_data: dict) -> None:
        client_order_id = order_data.get('client_order_id')
        status = order_data.get('status')
        symbol = order_data.get('symbol')

        self.logger.info(f"Processing order event: {client_order_id} ({status})")

        if symbol != self.currency_pair:
            self.logger.debug(f"Ignoring order for {symbol} (expected {self.currency_pair})")
            return

        async with self.pending_orders_lock:
            callback_info = self.pending_orders.pop(client_order_id, None)
            if callback_info:
                self.logger.debug(f"Found registered callback for {client_order_id}")
            else:
                self.logger.warning(f"Received orphan order event: {client_order_id}")

        enhanced_event = {
            **order_data,
            'client_order_id': client_order_id,
            'instance_index': callback_info[1] if callback_info else None,
            'filled': order_data.get('filled', 0),
            'amount': order_data.get('amount', 0)
        }

        if callback_info:
            callback, _ = callback_info
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(client_order_id, enhanced_event)
                else:
                    callback(client_order_id, enhanced_event)
                self.logger.debug(f"Executed callback for {client_order_id}")
            except Exception as e:
                self.logger.error(f"Callback failed for {client_order_id}: {str(e)}")

        self.on_order_callback(client_order_id, enhanced_event)

    async def _handle_reconnect_cleanup(self):
        self.logger.info("Performing reconnect state cleanup")
        if self.ws_manager:
            await self.ws_manager.clean_instance_states(*self.instance_range)
        self.logger.debug("Reconnect cleanup completed")

    async def async_send_message(self, message: dict) -> None:
        try:
            if self._connection and self._connection.open:
                msg_str = json.dumps(message)
                start = time.monotonic()
                await self._connection.send(msg_str)
                duration = (time.monotonic() - start) * 1000
                self.logger.debug(f"Sent message in {duration:.2f}ms (Length: {len(msg_str)} bytes)")
                self.logger.log(
                    logging.DEBUG-1,  # Lower than DEBUG
                    f"Message content: {msg_str[:200]}{'...' if len(msg_str) > 200 else ''}"
                )
            else:
                self.logger.error("Message send failed - connection not open")
        except Exception as e:
            self.logger.error(f"Message send error: {str(e)}", exc_info=self.logger.isEnabledFor(logging.DEBUG))

    async def register_pending_order(self,
                                    client_order_id: str,
                                    callback: Callable,
                                    instance_index: int) -> None:
        if self.is_event_connection:
            async with self.pending_orders_lock:
                self.pending_orders[client_order_id] = (callback, instance_index)
                self.logger.info(f"Registered pending order {client_order_id} for instance {instance_index}")

    async def close(self) -> None:
        self.logger.info("Closing WebSocket connection")
        self._keep_running = False
        if self._connection:
            await self._connection.close()

    async def start(self) -> None:
        self.logger.info("Starting WebSocket connection manager")
        asyncio.create_task(self.connect())

    async def _on_close(self) -> None:
        self.logger.info("Connection closed")
        self.logger.debug(
            f"Connection stats: "
            f"Attempts={self._connection_attempts}, "
            f"Messages Received={self._messages_received}, "
            f"Messages Processed={self._messages_processed}"
                )
