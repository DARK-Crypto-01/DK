import asyncio
import json
import time
import logging
import uuid
import secrets
from typing import Optional, Callable, Dict, Any
from exchange_adapter import ExchangeAdapter

class GenericWebSocketOrders:
    def __init__(self, ws_connection, adapter: ExchangeAdapter):
        self.ws_connection = ws_connection
        self.adapter = adapter
        self.logger = logging.getLogger("WSOrders")
        self.pending_confirmations = asyncio.Queue()
        
        # Config shortcuts
        self.order_cfg = self.adapter.exchange_cfg['order_params']
        self.msg_templates = self.adapter.exchange_cfg['message_templates']
        self.retry_cfg = self.adapter.strategy_cfg['retry_config']

        self.logger.info("Initializing WebSocket order manager")
        self.logger.debug(f"Order config: {json.dumps(self.order_cfg, indent=2)}")
        self.logger.debug(f"Message templates: {json.dumps(self.msg_templates, indent=2)}")

    def _generate_client_id(self) -> str:
        """Generate secure client order ID using cryptographic randomness"""
        self.logger.debug("Entering _generate_client_id")
        start_time = time.time()
        
        id_cfg = self.adapter.exchange_cfg.get('client_id', {})
        pattern = id_cfg.get('pattern', 'uuid')
        fmt = id_cfg.get('format', '')
        
        components = {
            'uuid': str(uuid.uuid4()),
            'millis': int(time.time() * 1000),
            'micros': int(time.time() * 1_000_000),
            'nanos': int(time.time() * 1_000_000_000),
            'timestamp': int(time.time()),
            'rand4': secrets.token_hex(2),  # 4-character hex
            'rand6': secrets.token_hex(3),  # 6-character hex
            'rand8': secrets.token_hex(4),  # 8-character hex (32-bit entropy)
            'exchange': self.adapter.exchange_cfg['name']
        }

        self.logger.debug(f"Generated ID components: {components}")

        try:
            if pattern == "uuid":
                result = fmt.format(**components) if fmt else components['uuid']
                self.logger.debug(f"Generated UUID client ID: {result}")
                
            elif pattern == "timestamp":
                if not fmt:
                    raise ValueError("Timestamp pattern requires format")
                
                # Auto-enhance format with entropy components
                required = ['micros', 'rand8']
                if not all(f"{{{c}}}" in fmt for c in required):
                    fmt += "_{micros}_{rand8}"
                    self.logger.debug("Auto-enhanced client ID format: %s", fmt)
                
                result = fmt.format(**components)
                self.logger.debug(f"Generated timestamp client ID: {result}")

        except KeyError as e:
            self.logger.error(f"Missing component in client ID format: {str(e)}")
            result = str(uuid.uuid4())  # Fallback
            self.logger.warning(f"Falling back to UUID: {result}")
            
        duration = time.time() - start_time
        self.logger.debug(f"Client ID generation completed in {duration:.4f}s")
        return result

    async def async_place_stop_limit_order_ws(self,
                                            order_type: str,
                                            trigger_price: float,
                                            limit_price: float,
                                            amount: float,
                                            instance_index: int = 0) -> str:
        """Place stop-limit order with type mapping"""
        self.logger.info(
            "Placing %s stop-limit order on instance %d: T=%.4f L=%.4f Amount=%.6f",
            order_type.upper(), instance_index, trigger_price, limit_price, amount
        )
        self.logger.debug(
            "Entering async_place_stop_limit_order_ws with params: "
            "order_type=%s, trigger=%.4f, limit=%.4f, amount=%.6f, instance=%d",
            order_type, trigger_price, limit_price, amount, instance_index
        )

        client_order_id = self._generate_client_id()
        order_type = order_type.lower()
        start_time = time.time()

        try:
            self.logger.debug("Constructing order message for %s", client_order_id)
            order_msg = await self._create_order_message(
                operation='create',
                order_type='stop_limit',
                client_order_id=client_order_id,
                order_params={
                    'trigger': trigger_price,
                    'limit': limit_price,
                    'amount': amount,
                    'side': order_type,
                    'instance_index': instance_index
                }
            )
            
            self.logger.debug("Registering pending order %s for instance %d", 
                            client_order_id, instance_index)
            await self.ws_connection.register_pending_order(
                client_order_id,
                self._async_order_confirmation_callback,
                instance_index
            )
            
            self.logger.debug("Sending order message: %s", json.dumps(order_msg))
            await self.ws_connection.async_send_message(order_msg)
            
            duration = time.time() - start_time
            self.logger.info(
                "Successfully submitted order %s in %.4fs", 
                client_order_id, duration
            )
            return client_order_id
            
        except Exception as e:
            self.logger.error(
                "Order placement failed for instance %d: %s",
                instance_index, str(e), exc_info=True
            )
            raise

    async def _create_order_message(self,
                                  operation: str,
                                  order_type: str,
                                  client_order_id: str,
                                  order_params: Dict[str, Any]) -> Dict:
        """Construct order message with exchange-specific types"""
        self.logger.debug(
            "Creating %s message for %s order %s",
            operation.upper(), order_type.upper(), client_order_id
        )
        
        template = self.msg_templates[f'order_{operation}']
        exchange_type = self.adapter.get_exchange_order_type(order_type)
        params = self._get_order_parameters(order_type, order_params)
        
        base_msg = {
            "time": int(time.time()),
            "channel": template['channel'],
            "event": template['event'],
            "payload": [{
                "client_order_id": client_order_id,
                "symbol": self.ws_connection.currency_pair,
                "side": order_params['side'],
                "type": exchange_type,  # Mapped order type
                **params
            }]
        }
        
        # Authentication
        auth_params = self._get_auth_parameters(template, base_msg['time'])
        base_msg["auth"] = self.adapter.create_auth_payload(auth_params)
        
        self.logger.debug(
            "Constructed %s message for %s: %s",
            operation.upper(), client_order_id, json.dumps(base_msg, indent=2)
        )
        return base_msg

    def _get_order_parameters(self, order_type: str, params: Dict) -> Dict:
        """Get parameters using mapped order type"""
        self.logger.debug(
            "Mapping order parameters for %s order: %s",
            order_type.upper(), params
        )
        
        exchange_type = self.adapter.get_exchange_order_type(order_type)
        cfg = self.order_cfg[exchange_type]
        
        if order_type == 'market':
            return {cfg['amount_key']: str(params['amount'])}
        
        base_params = {
            cfg['trigger_key']: str(params['trigger']),
            cfg['limit_key']: str(params['limit']),
            cfg['amount_key']: str(params['amount']),
        }
        
        if params.get('side', '').lower() == 'buy' and order_type != 'market':
            base_params["timeInForce"] = cfg.get('time_in_force', 'IOC')
            
        self.logger.debug(
            "Mapped parameters for %s order: %s",
            order_type.upper(), base_params
        )
        return base_params

    def _get_auth_parameters(self, template: Dict, timestamp: int) -> str:
        """Generate auth parameters"""
        self.logger.debug("Generating auth parameters for template: %s", template)
        
        if 'param_format' in self.msg_templates['auth']:
            params = self.msg_templates['auth']['param_format'].format(
                channel=template['channel'],
                event=template['event'],
                time=timestamp
            )
        else:
            params = f"channel={template['channel']}&event={template['event']}&time={timestamp}"
        
        self.logger.debug("Generated auth parameters: %s", params)
        return params

    async def async_amend_order_ws(self,
                                  client_order_id: str,
                                  new_trigger: float,
                                  new_limit: float,
                                  instance_index: int = 0) -> bool:
        """Amend existing order with security checks"""
        self.logger.info(
            "Amending order %s on instance %d: New T=%.4f L=%.4f",
            client_order_id, instance_index, new_trigger, new_limit
        )
        start_time = time.time()

        try:
            self.logger.debug(
                "Creating amend message for %s on instance %d",
                client_order_id, instance_index
            )
            amend_msg = await self._create_order_message(
                operation='amend',
                order_type='stop_limit',  # Original type maintained
                client_order_id=client_order_id,
                order_params={
                    'trigger': new_trigger,
                    'limit': new_limit,
                    'instance_index': instance_index
                }
            )
            
            self.logger.debug("Sending amend message: %s", json.dumps(amend_msg))
            await self.ws_connection.async_send_message(amend_msg)
            
            duration = time.time() - start_time
            self.logger.info(
                "Amendment request for %s completed in %.4fs", 
                client_order_id, duration
            )
            return True
        except Exception as e:
            self.logger.error(
                "Amendment failed for %s: %s",
                client_order_id, str(e), exc_info=True
            )
            return False

    async def async_cancel_order_ws(self,
                                   client_order_id: str,
                                   instance_index: int = 0) -> bool:
        """Cancel order with secure client ID"""
        self.logger.info("Initiating cancellation for %s on instance %d", 
                       client_order_id, instance_index)
        start_time = time.time()

        try:
            template = self.msg_templates['order_cancel']
            timestamp = int(time.time())
            
            msg = {
                "time": timestamp,
                "channel": template['channel'],
                "event": template['event'],
                "payload": [client_order_id],
                "auth": self.adapter.create_auth_payload(
                    self._get_auth_parameters(template, timestamp)
                )
            }
            
            self.logger.debug("Sending cancel message: %s", json.dumps(msg))
            await self.ws_connection.async_send_message(msg)
            
            duration = time.time() - start_time
            self.logger.info(
                "Cancel request for %s completed in %.4fs", 
                client_order_id, duration
            )
            return True
        except Exception as e:
            self.logger.error(
                "Cancellation failed for %s: %s",
                client_order_id, str(e), exc_info=True
            )
            return False

    async def async_place_market_order_ws(self,
                                         order_type: str,
                                         amount: float,
                                         instance_index: int = 0) -> str:
        """Place market order with type mapping"""
        self.logger.info(
            "Placing %s market order on instance %d: Amount=%.6f",
            order_type.upper(), instance_index, amount
        )
        start_time = time.time()

        try:
            client_order_id = self._generate_client_id()
            order_type = order_type.lower()

            self.logger.debug(
                "Creating market order message for %s", client_order_id
            )
            order_msg = await self._create_order_message(
                operation='create',
                order_type='market',
                client_order_id=client_order_id,
                order_params={
                    'amount': amount,
                    'side': order_type,
                    'instance_index': instance_index
                }
            )
            
            await self.ws_connection.register_pending_order(
                client_order_id,
                self._async_order_confirmation_callback,
                instance_index
            )
            
            self.logger.debug("Sending market order message: %s", json.dumps(order_msg))
            await self.ws_connection.async_send_message(order_msg)
            
            duration = time.time() - start_time
            self.logger.info(
                "Market order %s placed in %.4fs", client_order_id, duration
            )
            return client_order_id
        except Exception as e:
            self.logger.error(
                "Market order failed on instance %d: %s",
                instance_index, str(e), exc_info=True
            )
            raise

    async def _async_order_confirmation_callback(self, client_order_id: str, result: dict) -> None:
        """Handle order confirmations"""
        status = result.get('status')
        instance_index = result.get('instance_index')
        self.logger.debug(
            "Received confirmation for %s: Status=%s, Instance=%d",
            client_order_id, status, instance_index
        )

        if status in ["open", "new"]:
            self.logger.info("Order %s confirmed as %s", client_order_id, status)
            await self.pending_confirmations.put((client_order_id, True))
        elif status in ["rejected", "canceled"]:
            error = result.get('message', 'Unknown error')
            self.logger.warning(
                "Order %s failed: %s", client_order_id, error
            )
            await self.pending_confirmations.put((client_order_id, False))
        else:
            self.logger.debug(
                "Unhandled status for %s: %s", client_order_id, status
            )

    async def wait_for_confirmation(self,
                                   client_order_id: str,
                                   timeout: float = 10.0) -> bool:
        """Wait for order confirmation with timeout"""
        self.logger.debug(
            "Starting confirmation wait for %s (timeout: %.1fs)",
            client_order_id, timeout
        )
        start_time = time.time()

        try:
            async with asyncio.timeout(timeout):
                while True:
                    cid, status = await self.pending_confirmations.get()
                    self.logger.debug(
                        "Processing confirmation queue item: %s -> %s", cid, status
                    )
                    if cid == client_order_id:
                        duration = time.time() - start_time
                        self.logger.info(
                            "Confirmation received for %s in %.2fs: %s",
                            client_order_id, duration, status
                        )
                        return status
        except asyncio.TimeoutError:
            self.logger.warning(
                "Confirmation timeout for %s after %.1fs", 
                client_order_id, timeout
            )
            return False 
