import asyncio
import math
import logging
from datetime import datetime
from ws_manager import WSManager
from parallel_order_manager import ParallelOrderManager
from exchange_adapter import ExchangeAdapter

class OrderState:
    def __init__(self, config):
        self.active_orders = {}
        self.order_mapping = {}
        self.position_tracker = {}
        self.pending_actions = set()
        self.completed_counts = {}
        self.lock = asyncio.Lock()
        self.logger = logging.getLogger("OrderState")
        self.trade_limit = config['strategy']['trade_limit']

        self.logger.info("Initialized OrderState with trade limit: %d", self.trade_limit)

    async def clear_pending_range(self, start: int, end: int):
        async with self.lock:
            before = len(self.pending_actions)
            self.pending_actions = {
                idx for idx in self.pending_actions 
                if not (start <= idx <= end)
            }
            self.logger.debug("Cleared pending actions range %d-%d. Before: %d, After: %d",
                            start, end, before, len(self.pending_actions))

    async def atomic_state_snapshot(self):
        async with self.lock:
            snapshot = {
                'active_orders': dict(self.active_orders),
                'pending_actions': set(self.pending_actions),
                'positions': dict(self.position_tracker),
                'completed_counts': dict(self.completed_counts)
            }
            self.logger.debug("State snapshot created. Active orders: %d, Positions: %d",
                            len(snapshot['active_orders']), len(snapshot['positions']))
            return snapshot

class TradingStrategy:
    def __init__(self, config, adapter: ExchangeAdapter):
        self.config = config
        self.adapter = adapter
        self.state = OrderState(config)
        self.logger = logging.getLogger("TradingStrategy")
        self.price_initialized_event = asyncio.Event()
        self.price_update_event = asyncio.Event()
        self._current_price = None
        
        # Concurrency control locks
        self._price_lock = asyncio.Lock()
        self._state_operation_lock = asyncio.Lock()
        
        # Price update handling controls
        self.processing_lock = asyncio.Lock()
        self.pending_price_update = False
        self.latest_price = None
        
        # Initialize precision values
        self.tick_size = 10 ** -self.adapter.exchange_cfg['precision']['price']
        self.fee_precision = self.adapter.exchange_cfg['precision']['fee']
        
        # Get fixed parallel instances from config
        self.parallel_instances = config['strategy']['parallel_instances']
        
        # Initialize WebSocket manager
        self.logger.info("Initializing WSManager with %d parallel instances", self.parallel_instances)
        self.ws_manager = WSManager(
            currency_pair=self.adapter.format_symbol(
                config['strategy']['currency_pair']
            ),
            on_price_callback=self._price_update_handler,
            on_order_callback=self.on_order_event,
            api_key=self.adapter.exchange_cfg['api']['key'],
            api_secret=self.adapter.exchange_cfg['api']['secret'],
            config=config,
            total_instances=self.parallel_instances,
            max_instances_per_ws=config['websocket']['max_instances_per_ws']
        )

        # Initialize order manager
        self.logger.debug("Creating ParallelOrderManager instance")
        self.order_manager = ParallelOrderManager(
            strategy=self,
            state=self.state,
            config=config,
            ws_manager=self.ws_manager
        )

        # Start price initialization task
        asyncio.create_task(self._wait_for_initial_price())
        self.logger.info("TradingStrategy initialization complete")

    async def _wait_for_initial_price(self):
        """Wait for valid initial price from exchange"""
        self.logger.info("Starting price initialization monitoring")
        wait_attempts = 0
        while True:
            await self.price_initialized_event.wait()
            current_price = await self.get_current_price()
            wait_attempts += 1
            
            if self._is_valid_price(current_price):
                self.logger.info("Valid initial price obtained after %d attempts: %.4f",
                               wait_attempts, current_price)
                break
                
            self.logger.warning("Invalid initial price detected: %s. Resetting event.", current_price)
            self.price_initialized_event.clear()
        
        self.logger.info(f"Initial price confirmed: {current_price:.4f}")
        self.logger.debug("Starting order placement for %d parallel instances", self.parallel_instances)
        asyncio.create_task(
            self.order_manager.place_new_orders(
                self._calculate_prices,
                self.get_current_price,
                self.parallel_instances
            )
        )

    def _price_update_handler(self, price):
        """Handle price updates from centralized WS connection"""
        self.logger.debug("Raw price update received: %s", price)
        if not self._is_valid_price(price):
            self.logger.warning("Invalid price update ignored: %s", price)
            return
        asyncio.create_task(self._async_handle_price_update(price))

    async def _async_handle_price_update(self, price: float):
        """Process price updates with proper locking"""
        async with self._price_lock:
            raw_price = price
            price = round(price, self.adapter.exchange_cfg['precision']['price'])
            self.logger.debug("Processing price update. Raw: %.6f, Rounded: %.4f",
                            raw_price, price)
            
            if not self.price_initialized_event.is_set():
                self.logger.info("Setting initial price: %.4f", price)
                self._current_price = price
                self.price_initialized_event.set()
            elif price != self._current_price:
                async with self._state_operation_lock:
                    self.logger.debug("Price change detected: %.4f → %.4f (Δ%.4f)",
                                    self._current_price, price, price-self._current_price)
                    if not self.processing_lock.locked():
                        self._current_price = price
                        self.logger.debug("Triggering immediate price update event")
                        self.price_update_event.set()
                    else:
                        self.logger.debug("Queueing pending price update")
                        self.latest_price = price
                        self.pending_price_update = True

    def _is_valid_price(self, price) -> bool:
        try:
            valid = float(price) > 0
            if not valid:
                self.logger.warning("Negative price validation failure: %s", price)
            return valid
        except (TypeError, ValueError):
            self.logger.error("Price validation type error: %s", type(price).__name__)
            return False

    async def get_current_price(self) -> float:
        """Safe price access with locking"""
        async with self._price_lock:
            price = self._current_price
            self.logger.debug("Price access: %.4f", price)
            return price

    async def on_order_event(self, client_order_id, event):
        """Atomic order event processing"""
        async with self.state.lock:
            start_time = datetime.now()
            status = event.get('status')
            instance_index = event.get('instance_index')
            side = event.get('side')
            filled = float(event.get('filled', 0))
            orig_amount = float(event.get('amount', 0))

            self.logger.info("[%s] Order event: %s/%s (Instance %d)",
                           client_order_id[:8],
                           status,
                           side,
                           instance_index)

            # Handle sell order partial fills
            if side == 'sell' and filled > 0:
                fill_percent = (filled / orig_amount) * 100
                self.logger.info("[%s] Sell fill: %.2f/%s (%.1f%%)",
                               client_order_id[:8],
                               filled,
                               orig_amount,
                               fill_percent)
                
                counts = self.state.completed_counts.setdefault(
                    instance_index, {'buy':0, 'sell':0}
                )
                counts['sell'] += 1
                
                self.state.position_tracker.pop(instance_index, None)
                
                if instance_index in self.state.active_orders:
                    del self.state.active_orders[instance_index]
                if client_order_id in self.state.order_mapping:
                    del self.state.order_mapping[client_order_id]

                self.logger.debug("[%s] Scheduling buy replacement", client_order_id[:8])
                asyncio.create_task(
                    self._safe_replace_order(instance_index, 'buy')
                )
                return

            # Handle full fills and cancellations
            if status == "filled":
                count_type = 'buy' if side.lower() == 'buy' else 'sell'
                counts = self.state.completed_counts.setdefault(
                    instance_index, {'buy':0, 'sell':0}
                )
                counts[count_type] += 1

                if side == 'buy':
                    self.state.position_tracker[instance_index] = filled
                    self.logger.info("[%s] Buy filled. Position: %.6f",
                                  client_order_id[:8], filled)
                else:
                    self.state.position_tracker.pop(instance_index, None)
                    self.logger.info("[%s] Sell completed", client_order_id[:8])

            if status in ["filled", "canceled"]:
                self.logger.debug("[%s] Cleaning up order state", client_order_id[:8])
                self.state.active_orders.pop(instance_index, None)
                self.state.order_mapping.pop(client_order_id, None)
                
                if side == 'sell':
                    self.logger.debug("[%s] Scheduling sell replacement", client_order_id[:8])
                    asyncio.create_task(
                        self._safe_replace_order(instance_index, 'buy')
                    )

            duration = (datetime.now() - start_time).total_seconds() * 1000
            self.logger.debug("Order event processed in %.2fms", duration)

    async def _safe_replace_order(self, instance_index, new_type):
        """Order replacement with safety checks"""
        self.logger.debug("Starting replacement check for instance %d (%s)",
                        instance_index, new_type)
        if not await self._should_continue_trading(instance_index):
            self.logger.info("Instance %d reached trade limit", instance_index)
            return
            
        async with self._state_operation_lock:
            if not self.state.position_tracker.get(instance_index) and new_type == 'sell':
                self.logger.warning("Attempted sell replacement with no position on instance %d",
                                  instance_index)
                return

            try:
                self.logger.info("Initiating %s order replacement on instance %d",
                               new_type.upper(), instance_index)
                await self.order_manager.place_single_order(
                    instance_index,
                    self._calculate_prices,
                    self.get_current_price,
                    new_type
                )
            except Exception as e:
                self.logger.error("Replacement failed for instance %d: %s",
                               instance_index, str(e), exc_info=True)

    def _calculate_prices(self, last_price, order_type, instance_index=0):
        cfg = self.config['trading']['buy' if order_type == 'buy' else 'sell']
        tick = self.tick_size
        
        if order_type == 'buy':
            t_adj = cfg['trigger_price_adjust']
            l_adj = cfg['limit_price_adjust']
            trigger = last_price + (t_adj + instance_index) * tick
            limit = last_price + (l_adj + instance_index) * tick
        else:
            t_adj = cfg['trigger_price_adjust']
            l_adj = cfg['limit_price_adjust']
            trigger = last_price - (t_adj + instance_index) * tick
            limit = last_price - (l_adj + instance_index) * tick

        calculated = (
            round(trigger, self.adapter.exchange_cfg['precision']['price']),
            round(limit, self.adapter.exchange_cfg['precision']['price'])
        )
        
        self.logger.debug("Calculated prices for %s (instance %d): T=%.4f L=%.4f",
                        order_type.upper(), instance_index, *calculated)
        return calculated

    async def _should_continue_trading(self, instance_index: int) -> bool:
        """Atomic check for trade continuation"""
        async with self.state.lock:
            counts = self.state.completed_counts.get(instance_index, {'buy':0, 'sell':0})
            total = sum(counts.values())
            allowed = total < self.config['strategy']['trade_limit']
            self.logger.debug("Trade continuation check for %d: %s (%d/%d)",
                            instance_index, allowed, total, self.config['strategy']['trade_limit'])
            return allowed

    async def manage_strategy(self):
        """Main strategy loop"""
        self.logger.info("Starting main strategy management loop")
        await self.price_initialized_event.wait()
        
        loop_count = 0
        while True:
            loop_start = datetime.now()
            async with self.processing_lock:
                self.logger.debug("Strategy loop iteration %d started", loop_count)
                await self.price_update_event.wait()
                self.price_update_event.clear()
                
                # Use latest price if available
                current_price = await self.get_current_price()
                if self.latest_price is not None:
                    self.logger.debug("Using queued price update: %.4f → %.4f",
                                    current_price, self.latest_price)
                    current_price = self.latest_price
                    self.latest_price = None
                
                if not self._is_valid_price(current_price):
                    self.logger.warning("Skipping processing due to invalid price: %s", current_price)
                    continue
                    
                try:
                    self.logger.info("Starting order monitoring cycle")
                    monitor_start = datetime.now()
                    await self.order_manager.async_monitor_active_orders(
                        self.get_current_price,
                        self._calculate_prices
                    )
                    monitor_duration = (datetime.now() - monitor_start).total_seconds() * 1000
                    self.logger.debug("Order monitoring completed in %.2fms", monitor_duration)
                except Exception as e:
                    self.logger.error("Order monitoring error: %s", str(e), exc_info=True)
                
                # Check for pending updates and re-trigger
                if self.pending_price_update:
                    self.logger.debug("Processing pending price update flag")
                    self.pending_price_update = False
                    self.price_update_event.set()

                loop_duration = (datetime.now() - loop_start).total_seconds() * 1000
                self.logger.debug("Strategy loop iteration %d completed in %.2fms",
                                loop_count, loop_duration)
                loop_count += 1

    async def shutdown(self):
        """Graceful shutdown procedure"""
        self.logger.info("Initiating shutdown sequence")
        shutdown_start = datetime.now()
        
        await self.order_manager.async_graceful_shutdown()
        await self.ws_manager.close_all()
        
        shutdown_duration = (datetime.now() - shutdown_start).total_seconds()
        self.logger.info("Shutdown completed in %.2f seconds", shutdown_duration) 
