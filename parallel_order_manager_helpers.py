import asyncio
import math
import logging
from typing import Callable, Dict, Tuple, Optional
from functools import partial

class OrderHelpers:
    def __init__(self, strategy, state, config, ws_manager):
        self.strategy = strategy
        self.state = state
        self.config = config
        self.ws_manager = ws_manager
        self.logger = logging.getLogger("OrderHelpers")
        self._log = partial(
            self._enhanced_log,
            class_name=self.__class__.__name__
        )
        
    def _enhanced_log(self, level: str, message: str, 
                     class_name: str, method_name: str = None, **kwargs):
        """Enhanced logging with structured context"""
        log_method = getattr(self.logger, level)
        context = {
            'class': class_name,
            'method': method_name or 'unknown',
            **kwargs
        }
        log_msg = f"[{class_name}]"
        if method_name:
            log_msg += f".{method_name}"
        log_msg += f" - {message} - Context: {context}"
        log_method(log_msg)

    def _truncate_to_precision(self, value: float, precision: int) -> float:
        """Pure function - no state access, remains unchanged"""
        self._log('debug', "Truncating value to precision", 
                 method_name='_truncate_to_precision',
                 input_value=value, target_precision=precision)
        
        if not value or value <= 0:
            self._log('debug', "Zero or negative value detected", 
                      method_name='_truncate_to_precision',
                      result_value=0.0)
            return 0.0
            
        factor = 10 ** precision
        result = int(value * factor) / factor
        self._log('debug', "Value truncated successfully",
                 method_name='_truncate_to_precision',
                 result_value=result)
        return result

    async def generate_order_parameters(self, 
                                      instance_index: int,
                                      get_price_func: Callable,
                                      calculate_prices: Callable,
                                      order_type: str) -> Dict:
        """Atomic parameter generation with state locking"""
        self._log('debug', "Generating order parameters",
                 method_name='generate_order_parameters',
                 instance_index=instance_index, order_type=order_type)
        
        async with self.state.lock:
            current_price = await get_price_func()
            trigger, limit = calculate_prices(current_price, order_type, instance_index)
            
            self._log('info', "Price parameters calculated",
                     method_name='generate_order_parameters',
                     instance_index=instance_index,
                     current_price=current_price,
                     trigger_price=trigger,
                     limit_price=limit)

            amount = None
            if order_type == 'sell':
                bought_amount = self.state.position_tracker.get(instance_index, 0)
                self._log('debug', "Sell order amount calculation",
                         method_name='generate_order_parameters',
                         instance_index=instance_index,
                         position_amount=bought_amount)
                
                if bought_amount > 0:
                    fee_rate = self.config['trading'].get('sell_trading_fee', 0.001)
                    raw_fee = bought_amount * fee_rate
                    amount_after_fee = bought_amount - raw_fee
                    amount_precision = self.strategy.adapter.exchange_cfg['precision']['amount']
                    truncated_amount = self._truncate_to_precision(amount_after_fee, amount_precision)
                    amount = max(0, truncated_amount)
                    
                    self._log('info', "Sell amount after fee calculation",
                             method_name='generate_order_parameters',
                             instance_index=instance_index,
                             raw_amount=bought_amount,
                             fee_rate=fee_rate,
                             amount_after_fee=amount)
                else:
                    self._log('warning', "No position to sell",
                             method_name='generate_order_parameters',
                             instance_index=instance_index)
            elif order_type == 'buy':
                amount = self.strategy.calculate_order_amount(limit)
                self._log('info', "Buy order amount calculated",
                         method_name='generate_order_parameters',
                         instance_index=instance_index,
                         limit_price=limit,
                         calculated_amount=amount)
                
            return {
                'price': current_price,
                'trigger': trigger,
                'limit': limit,
                'amount': amount
            }

    async def async_execute_ws_operation(self,
                                        operation: str,
                                        instance_index: int,
                                        calculate_prices: Callable,
                                        get_price: Callable,
                                        order_type: str,
                                        client_order_id: Optional[str] = None) -> Tuple[Optional[str], Dict]:
        """Atomic operation execution with full state protection"""
        self._log('info', "Initiating WS operation",
                 method_name='async_execute_ws_operation',
                 operation=operation,
                 instance_index=instance_index,
                 order_type=order_type,
                 client_order_id=client_order_id)

        retry_type = 'order_amendment' if operation == 'amend' else 'order_placement'
        retry_cfg = self.config['retry_config'].get(retry_type, {})
        
        max_retries = retry_cfg.get('max_retries', 0)
        first_retry_immediate = retry_cfg.get('first_retry_immediate', False)
        multiplier = retry_cfg.get('multiplier', 1.5)
        logging_only = retry_cfg.get('logging_only', False)
        base_interval = retry_cfg.get('initial_interval', 
                                    0.1 if retry_type == 'order_placement' else 0)

        async with self.state.lock:
            if instance_index in self.state.pending_actions:
                self._log('warning', "Instance has pending actions - aborting",
                         method_name='async_execute_ws_operation',
                         instance_index=instance_index)
                return None, {}
            self.state.pending_actions.add(instance_index)

        try:
            for attempt in range(max_retries + 1):
                try:
                    self._log('debug', "Parameter generation attempt",
                             method_name='async_execute_ws_operation',
                             attempt=attempt+1,
                             max_attempts=max_retries+1)

                    async with self.state.lock:
                        params = await self.generate_order_parameters(
                            instance_index, get_price, calculate_prices, order_type
                        )
                        if params['amount'] is None or params['amount'] <= 0:
                            error_msg = "Invalid order amount calculated"
                            self._log('error', error_msg,
                                     method_name='async_execute_ws_operation',
                                     params=params)
                            raise ValueError(error_msg)

                    if operation == 'place':
                        result = await self.async_place_order_via_ws(
                            instance_index, order_type, params
                        )
                        self._log('info', "Order placement successful",
                                 method_name='async_execute_ws_operation',
                                 instance_index=instance_index,
                                 order_type=order_type)
                        return result, params
                    elif operation == 'amend':
                        result = await self.async_amend_order_via_ws(
                            instance_index, client_order_id, params
                        )
                        self._log('info', "Order amendment successful",
                                 method_name='async_execute_ws_operation',
                                 instance_index=instance_index,
                                 client_order_id=client_order_id)
                        return result, params

                except Exception as e:
                    if attempt >= max_retries or logging_only:
                        self._log('error', "Final operation failure",
                                 method_name='async_execute_ws_operation',
                                 exception=str(e),
                                 exc_info=True)
                        raise
                    
                    sleep_time = 0
                    if attempt == 0 and first_retry_immediate:
                        self._log('debug', "Immediate first retry",
                                 method_name='async_execute_ws_operation')
                    else:
                        adjusted_attempt = attempt - (1 if first_retry_immediate else 0)
                        sleep_time = base_interval * (multiplier ** adjusted_attempt)
                    
                    self._log('warning', "Operation retry scheduled",
                             method_name='async_execute_ws_operation',
                             attempt=attempt+1,
                             max_retries=max_retries,
                             sleep_time=sleep_time)
                    await asyncio.sleep(sleep_time)
        finally:
            async with self.state.lock:
                self.state.pending_actions.discard(instance_index)
                self._log('debug', "Pending action cleared",
                         method_name='async_execute_ws_operation',
                         instance_index=instance_index)

    async def async_place_order_via_ws(self,
                                      instance_index: int,
                                      order_type: str,
                                      params: Dict) -> Optional[str]:
        """Atomic order placement with state validation"""
        self._log('info', "Placing order via WS",
                 method_name='async_place_order_via_ws',
                 instance_index=instance_index,
                 order_type=order_type,
                 params=params)

        async with self.state.lock:
            current_state = await self.state.atomic_state_snapshot()
            if instance_index in current_state['active_orders']:
                self._log('warning', "Active order exists - skipping placement",
                         method_name='async_place_order_via_ws',
                         instance_index=instance_index)
                return None

        try:
            ws_client = self.ws_manager.get_ws_client(instance_index)
            result = await ws_client.async_place_stop_limit_order_ws(
                order_type=order_type,
                trigger_price=params['trigger'],
                limit_price=params['limit'],
                amount=params['amount'],
                instance_index=instance_index
            )
            self._log('info', "Order placement successful",
                     method_name='async_place_order_via_ws',
                     instance_index=instance_index,
                     client_order_id=result)
            return result
        except Exception as e:
            self._log('error', "Order placement failed",
                     method_name='async_place_order_via_ws',
                     instance_index=instance_index,
                     exception=str(e),
                     exc_info=True)
            raise

    async def async_amend_order_via_ws(self,
                                      instance_index: int,
                                      client_order_id: str,
                                      params: Dict) -> bool:
        """Atomic amendment with state consistency check"""
        self._log('info', "Amending order via WS",
                 method_name='async_amend_order_via_ws',
                 instance_index=instance_index,
                 client_order_id=client_order_id,
                 new_trigger=params['trigger'],
                 new_limit=params['limit'])

        async with self.state.lock:
            current_state = await self.state.atomic_state_snapshot()
            if not current_state['active_orders'].get(instance_index):
                self._log('warning', "No active order to amend",
                         method_name='async_amend_order_via_ws',
                         instance_index=instance_index)
                return False

        try:
            ws_client = self.ws_manager.get_ws_client(instance_index)
            success = await ws_client.async_amend_order_ws(
                client_order_id=client_order_id,
                new_trigger=params['trigger'],
                new_limit=params['limit'],
                instance_index=instance_index
            )
            self._log('info', "Order amendment result",
                     method_name='async_amend_order_via_ws',
                     success=success,
                     instance_index=instance_index)
            return success
        except Exception as e:
            self._log('error', "Order amendment failed",
                     method_name='async_amend_order_via_ws',
                     instance_index=instance_index,
                     exception=str(e),
                     exc_info=True)
            raise

    def needs_amendment(self, order_state: Dict, current_price: float) -> bool:
        """Pure function - no state access, remains unchanged"""
        price_diff = current_price - order_state['last_price']
        order_type = order_state.get('order_type', 'buy')
        amendment_needed = (price_diff < 0) if order_type == 'buy' else (price_diff > 0)
        
        self._log('debug', "Amendment check result",
                 method_name='needs_amendment',
                 order_type=order_type,
                 price_diff=price_diff,
                 amendment_needed=amendment_needed)
        return amendment_needed

    async def async_cancel_order_via_ws(self,
                                       instance_index: int,
                                       client_order_id: str) -> bool:
        """Atomic cancellation with state check"""
        self._log('info', "Initiating order cancellation",
                 method_name='async_cancel_order_via_ws',
                 instance_index=instance_index,
                 client_order_id=client_order_id)

        async with self.state.lock:
            current_state = await self.state.atomic_state_snapshot()
            if instance_index not in current_state['active_orders']:
                self._log('warning', "No active order to cancel",
                         method_name='async_cancel_order_via_ws',
                         instance_index=instance_index)
                return False

        try:
            ws_client = self.ws_manager.get_ws_client(instance_index)
            result = await ws_client.async_cancel_order_ws(
                order_id=client_order_id,
                instance_index=instance_index
            )
            self._log('info', "Cancellation result",
                     method_name='async_cancel_order_via_ws',
                     success=result,
                     instance_index=instance_index)
            return result
        except Exception as e:
            self._log('error', "Cancellation failed",
                     method_name='async_cancel_order_via_ws',
                     instance_index=instance_index,
                     exception=str(e),
                     exc_info=True)
            raise
