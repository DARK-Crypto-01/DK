import asyncio
import logging
import time
from functools import wraps
from typing import Callable, Optional
from parallel_order_manager_helpers import OrderHelpers

def log_method_call(func):
    """Decorator to log method entry/exit and execution time"""
    @wraps(func)
    async def async_wrapper(self, *args, **kwargs):
        self.logger.debug(f"{self.__class__.__name__}::{func.__name__} ENTRY - Args: {args} | Kwargs: {kwargs}")
        start_time = time.time()
        try:
            result = await func(self, *args, **kwargs)
            duration = time.time() - start_time
            self.logger.debug(f"{self.__class__.__name__}::{func.__name__} EXIT [OK] - Duration: {duration:.4f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"{self.__class__.__name__}::{func.__name__} EXIT [ERROR] - Duration: {duration:.4f}s | Error: {str(e)}", exc_info=True)
            raise
    return async_wrapper

class ParallelOrderManager:
    def __init__(self, strategy, state, config, ws_manager):
        self.strategy = strategy
        self.state = state
        self.config = config
        self.ws_manager = ws_manager
        self.helpers = OrderHelpers(strategy, state, config, ws_manager)
        self.logger = logging.getLogger("ParallelOrderManager")
        self._shutdown_flag = asyncio.Event()
        
        # Initialize performance metrics
        self.metrics = {
            'orders_placed': 0,
            'orders_amended': 0,
            'order_errors': 0,
            'avg_placement_time': 0.0
        }

    @log_method_call
    async def place_new_orders(self, 
                             calculate_prices: Callable,
                             get_price: Callable,
                             total_instances: int) -> None:
        """Initialize new orders across all fixed instances"""
        self.logger.info(f"Starting order placement for {total_instances} instances")
        
        tasks = []
        for instance_index in range(total_instances):
            async with self.state.lock:
                state_snapshot = await self.state.atomic_state_snapshot()
                if instance_index not in state_snapshot['pending_actions']:
                    tasks.append(
                        self.async_place_single_order(
                            instance_index, calculate_prices, get_price
                        )
                    )
        
        self.logger.debug(f"Created {len(tasks)} placement tasks from {total_instances} instances")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = 0
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                self.metrics['order_errors'] += 1
                self.logger.error(f"Order placement failed for instance {idx}: {str(result)}", exc_info=True)
            elif result is not None:
                success_count += 1
        
        self.logger.info(f"Order placement completed - Success: {success_count}/{len(tasks)} | Errors: {len(results)-success_count}")
        self.metrics['orders_placed'] += success_count

    @log_method_call
    async def async_place_single_order(self, 
                                      instance_index: int,
                                      calculate_prices: Callable,
                                      get_price: Callable) -> None:
        """Atomic single order placement with state locking"""
        if not await self.strategy._should_continue_trading(instance_index):
            self.logger.debug(f"Skipping instance {instance_index} - Reached order limit")
            return

        async with self.state.lock:
            state_snapshot = await self.state.atomic_state_snapshot()
            if instance_index in state_snapshot['active_orders']:
                self.logger.debug(f"Skipping instance {instance_index} - Existing active order")
                return
            self.state.pending_actions.add(instance_index)
            self.logger.debug(f"Added instance {instance_index} to pending actions")

        try:
            start_time = time.time()
            order_type = 'buy'
            self.logger.info(f"Preparing {order_type} order for instance {instance_index}")
            
            order, params = await self.helpers.async_execute_ws_operation(
                'place', instance_index, calculate_prices, get_price, order_type
            )
            
            if order:
                async with self.state.lock:
                    self.state.active_orders[instance_index] = {
                        'client_order_id': order,
                        'last_price': params['price'],
                        'limit_price': params['limit'],
                        'order_type': order_type
                    }
                    self.state.order_mapping[order] = instance_index
                    
                exec_time = time.time() - start_time
                self.metrics['avg_placement_time'] = (
                    (self.metrics['avg_placement_time'] * self.metrics['orders_placed'] + exec_time) 
                    / (self.metrics['orders_placed'] + 1)
                )
                self.metrics['orders_placed'] += 1
                
                self.logger.info(
                    f"Successfully placed {order_type} order on instance {instance_index} | "
                    f"Price: {params['price']:.4f} | Limit: {params['limit']:.4f} | "
                    f"Duration: {exec_time:.4f}s"
                )
        except Exception as e:
            self.metrics['order_errors'] += 1
            self.logger.error(
                f"Order placement failed for instance {instance_index} | Error: {str(e)}",
                exc_info=True
            )
            raise
        finally:
            async with self.state.lock:
                self.state.pending_actions.discard(instance_index)
                self.logger.debug(f"Removed instance {instance_index} from pending actions")

    @log_method_call
    async def async_monitor_active_orders(self,
                                        get_price: Callable,
                                        calculate_prices: Callable) -> None:
        """Atomic order monitoring with state consistency checks"""
        current_price = await get_price()
        self.logger.debug(f"Starting order monitoring at price: {current_price:.4f}")
        
        async with self.state.lock:
            state_snapshot = await self.state.atomic_state_snapshot()
            
        active_order_count = len(state_snapshot['active_orders'])
        self.logger.info(f"Monitoring {active_order_count} active orders")
        
        tasks = []
        for instance_index, order_state in state_snapshot['active_orders'].items():
            async with self.state.lock:
                current_state = await self.state.atomic_state_snapshot()
                if instance_index not in current_state['pending_actions']:
                    tasks.append(
                        self.async_check_and_amend_order(
                            instance_index, order_state, current_price, calculate_prices
                        )
                    )
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        amendments = sum(1 for r in results if r is True)
        self.logger.info(f"Order monitoring complete - Amendments attempted: {amendments}/{len(tasks)}")

    @log_method_call
    async def async_check_and_amend_order(self,
                                        instance_index: int,
                                        order_state: dict,
                                        current_price: float,
                                        calculate_prices: Callable) -> None:
        """Safe amendment check with state validation"""
        self.logger.debug(
            f"Checking order for instance {instance_index} | "
            f"Current: {current_price:.4f} | Order price: {order_state['last_price']:.4f}"
        )
        
        if self.helpers.needs_amendment(order_state, current_price):
            self.logger.info(
                f"Amendment needed for instance {instance_index} | "
                f"Price difference: {current_price - order_state['last_price']:.4f}"
            )
            try:
                await self.async_amend_order(instance_index, lambda: current_price, calculate_prices)
                self.metrics['orders_amended'] += 1
                return True
            except Exception as e:
                self.metrics['order_errors'] += 1
                self.logger.error(f"Amendment failed for instance {instance_index}: {str(e)}")
                return False
        return False

    @log_method_call
    async def async_amend_order(self, 
                               instance_index: int,
                               get_price: Callable,
                               calculate_prices: Callable) -> None:
        """Atomic order amendment process"""
        async with self.state.lock:
            state_snapshot = await self.state.atomic_state_snapshot()
            order_state = state_snapshot['active_orders'].get(instance_index)
            if not order_state:
                self.logger.warning(f"No active order to amend on instance {instance_index}")
                return

        self.logger.info(
            f"Amending order for instance {instance_index} | "
            f"Original price: {order_state['last_price']:.4f} | "
            f"Original limit: {order_state['limit_price']:.4f}"
        )
        
        try:
            amendment, params = await self.helpers.async_execute_ws_operation(
                operation='amend',
                instance_index=instance_index,
                calculate_prices=calculate_prices,
                get_price=get_price,
                order_type=order_state['order_type'],
                client_order_id=order_state['client_order_id']
            )

            if amendment:
                async with self.state.lock:
                    current_state = await self.state.atomic_state_snapshot()
                    if instance_index in current_state['active_orders']:
                        self.state.active_orders[instance_index].update({
                            'last_price': params['price'],
                            'limit_price': params['limit']
                        })
                self.logger.info(
                    f"Successfully amended order for instance {instance_index} | "
                    f"New price: {params['price']:.4f} | New limit: {params['limit']:.4f}"
                )
                return True
        except Exception as e:
            self.metrics['order_errors'] += 1
            self.logger.error(f"Amendment failed for instance {instance_index}: {str(e)}")
            raise

    @log_method_call
    async def async_graceful_shutdown(self) -> None:
        """Atomic shutdown procedure with state locking"""
        self.logger.info("Initiating graceful shutdown - Collecting final metrics...")
        self.logger.info(
            f"Performance metrics - "
            f"Orders placed: {self.metrics['orders_placed']} | "
            f"Amendments: {self.metrics['orders_amended']} | "
            f"Errors: {self.metrics['order_errors']} | "
            f"Avg placement time: {self.metrics['avg_placement_time']:.4f}s"
        )
        
        async with self.state.lock:
            total_position = sum(self.state.position_tracker.values())
            active_instances = [
                idx for idx, counts in self.state.completed_counts.items()
                if sum(counts.values()) < self.config['trading']['trade_limit']
            ]
            
        self.logger.info(
            f"Shutdown status - "
            f"Active instances: {len(active_instances)} | "
            f"Total position: {total_position:.6f}"
        )
        
        if active_instances and total_position > 0:
            try:
                fee_rate = self.config['trading'].get('sell_trading_fee', 0.001)
                amount_after_fee = total_position - (total_position * fee_rate)
                amount_precision = self.strategy.adapter.exchange_cfg['precision']['amount']
                truncated_amount = self.helpers._truncate_to_precision(amount_after_fee, amount_precision)
                liquidate_amount = max(0, truncated_amount)

                self.logger.info(
                    f"Initiating final liquidation - "
                    f"Amount: {liquidate_amount:.6f} | "
                    f"Pre-fee amount: {total_position:.6f} | "
                    f"Fee rate: {fee_rate*100:.2f}%"
                )
                
                ws_client = self.ws_manager.get_ws_client(0)
                await ws_client.async_place_market_order_ws(
                    'sell', 
                    liquidate_amount
                )
                self.logger.info(f"Final market sell order placed for {liquidate_amount:.6f}")
            except Exception as e:
                self.logger.error(f"Final liquidation failed: {str(e)}", exc_info=True)

        async with self.state.lock:
            cleared_orders = len(self.state.active_orders)
            cleared_positions = len(self.state.position_tracker)
            self.state.pending_actions.clear()
            self.state.active_orders.clear()
            self.state.position_tracker.clear()
            
        self.logger.info(
            f"State cleared - "
            f"Removed {cleared_orders} active orders | "
            f"Cleared {cleared_positions} positions"
        )

    @log_method_call
    async def async_recover_state(self):
        """State recovery with locking"""
        async with self.state.lock:
            self.logger.warning("State recovery initiated - This should be customized!")
            # Implement custom recovery logic here 
