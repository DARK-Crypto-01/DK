import math
import logging
from datetime import datetime
from ws_connection import GenericWebSocketConnection
from ws_orders import GenericWebSocketOrders
from exchange_adapter import ExchangeAdapter

class WSManager:
    def __init__(self, currency_pair, on_price_callback, on_order_callback, 
                 api_key, api_secret, config, total_instances, max_instances_per_ws):
        self.logger = logging.getLogger("WSManager")
        self.logger.info("Initializing WSManager")
        self.logger.debug(f"Parameters: currency_pair={currency_pair}, total_instances={total_instances}, "
                         f"max_instances_per_ws={max_instances_per_ws}")
        
        self.currency_pair = currency_pair
        self.on_price_callback = on_price_callback
        self.on_order_callback = on_order_callback
        self.api_key = api_key
        self.api_secret = api_secret
        self.config = config
        self.max_instances_per_ws = max_instances_per_ws
        self.total_instances = total_instances
        self.order_connections = []
        self.strategy = None
        self.event_connection = None

        self._create_connections()

    def _create_connections(self):
        exchange_name = self.config['strategy']['default_exchange']
        self.logger.debug(f"Creating connections for exchange: {exchange_name}")
        
        # Create event connection with enhanced logging
        try:
            event_adapter = ExchangeAdapter(self.config, exchange_name)
            self.logger.info("Creating event WebSocket connection")
            self.event_connection = GenericWebSocketConnection(
                adapter=event_adapter,
                on_price_callback=self.on_price_callback,
                on_order_callback=self._enhanced_order_callback,
                instance_range=(0, 0),
                ws_manager=self,
                is_event_connection=True
            )
            self.event_connection.start()
            self.logger.debug("Event connection started successfully")
        except Exception as e:
            self.logger.error(f"Failed to create event connection: {str(e)}")
            raise

        # Create order connections with instance distribution logging
        num_clients = math.ceil(self.total_instances / self.max_instances_per_ws)
        self.logger.info(f"Creating {num_clients} order WS clients for {self.total_instances} instances")
        
        for i in range(num_clients):
            start = i * self.max_instances_per_ws
            end = start + self.max_instances_per_ws - 1
            end = min(end, self.total_instances - 1)
            self.logger.debug(f"Client {i}: Handling instances {start}-{end}")

            try:
                order_adapter = ExchangeAdapter(self.config, exchange_name)
                order_conn = GenericWebSocketConnection(
                    adapter=order_adapter,
                    on_price_callback=lambda _: None,
                    on_order_callback=lambda *_: None,
                    instance_range=(start, end),
                    ws_manager=self,
                    is_event_connection=False
                )
                order_conn.start()
                self.order_connections.append(GenericWebSocketOrders(order_conn))
                self.logger.debug(f"Order client {i} initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to create order client {i}: {str(e)}")
                raise

    def _enhanced_order_callback(self, order_id, event):
        self.logger.debug(f"Received order event: {order_id} | Status: {event.get('status')} | "
                         f"Instance: {event.get('instance_index')}")
        if event.get('client_order_id'):
            self.logger.info(f"Processing valid order event: {order_id}")
            self.on_order_callback(order_id, event)
        else:
            self.logger.warning("Received invalid order event without client_order_id")
            self.logger.debug(f"Raw event data: {event}")

    async def clean_instance_states(self, start_idx: int, end_idx: int):
        self.logger.info(f"Cleaning states for instances {start_idx}-{end_idx}")
        self.logger.debug(f"Current strategy state: {self.strategy is not None}")
        
        if self.strategy and self.strategy.state:
            await self.strategy.state.clear_pending_range(start_idx, end_idx)
            self.logger.debug(f"Cleared pending actions for instances {start_idx}-{end_idx}")

    def get_ws_client(self, instance_index=None):
        if instance_index is None:
            self.logger.debug("Getting default WS client")
            return self.order_connections[0]
            
        client_index = instance_index // self.max_instances_per_ws
        client_index = min(client_index, len(self.order_connections)-1)
        self.logger.debug(f"Instance {instance_index} routed to WS client {client_index}")
        return self.order_connections[client_index]

    async def close_all(self):
        self.logger.info("Initiating shutdown of all WS connections")
        
        try:
            if self.event_connection:
                self.logger.debug("Closing event connection")
                await self.event_connection.close()
            
            self.logger.info(f"Closing {len(self.order_connections)} order connections")
            for idx, conn in enumerate(self.order_connections):
                self.logger.debug(f"Closing order connection {idx}")
                await conn.ws_connection.close()
            
            self.logger.info("All connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error during shutdown: {str(e)}")
            raise

    def get_all_clients(self):
        self.logger.debug(f"Returning {len(self.order_connections)} order clients")
        return self.order_connections 
