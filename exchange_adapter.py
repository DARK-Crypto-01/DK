import hashlib
import hmac
import json
import logging
import secrets
import time
from jsonpath_ng import parse
from typing import Dict

class SecurityError(Exception):
    """Custom exception for security validation failures"""
    pass

class ExchangeAdapter:
    def __init__(self, config, exchange_name):
        self.exchange_cfg = config['exchanges'][exchange_name]
        self.strategy_cfg = config['strategy']
        self.logger = logging.getLogger(f"ExchangeAdapter.{exchange_name}")
        self.order_type_mapping = self.exchange_cfg['order_types']
        
        self.logger.info("Initializing exchange adapter for %s", exchange_name)
        self.logger.debug("Configuration loaded: %s", self._masked_config())
        
        try:
            self.validate_client_id_config()
            self.logger.debug("Client ID configuration validated successfully")
        except Exception as e:
            self.logger.critical("Client ID validation failed: %s", str(e))
            raise

    def _masked_config(self):
        """Mask sensitive configuration values for logging"""
        masked = self.exchange_cfg.copy()
        masked['api']['key'] = '***' if masked['api'].get('key') else None
        masked['api']['secret'] = '***' if masked['api'].get('secret') else None
        return masked

    def validate_client_id_config(self):
        """Comprehensive security validation for client ID configuration"""
        start_time = time.perf_counter()
        self.logger.debug("Starting client ID configuration validation")
        
        if 'client_id' not in self.exchange_cfg:
            self.logger.debug("No client ID configuration found")
            return

        id_cfg = self.exchange_cfg['client_id']
        valid_patterns = ['uuid', 'timestamp']
        MIN_ENTROPY_BITS = 64

        self.logger.debug("Validating client ID pattern: %s", id_cfg.get('pattern'))
        
        if 'pattern' not in id_cfg:
            self.logger.error("Missing 'pattern' in client_id configuration")
            raise ValueError("Missing 'pattern' in client_id configuration")
            
        if id_cfg['pattern'] not in valid_patterns:
            self.logger.error("Invalid client_id pattern '%s'", id_cfg['pattern'])
            raise ValueError(f"Invalid client_id pattern '{id_cfg['pattern']}'")

        if id_cfg['pattern'] == 'timestamp':
            self.logger.debug("Validating timestamp-based client ID format")
            if not id_cfg.get('format'):
                self.logger.error("Missing format for timestamp client_id pattern")
                raise ValueError("Missing format for timestamp client_id pattern")
            
            fmt = id_cfg['format']
            components = [c[1:-1] for c in fmt.split('_') if c.startswith('{')]
            
            if any(c not in allowed_chars for c in fmt):
                self.logger.error("Invalid characters in client ID format")
                raise ValueError("Client ID format contains invalid characters")

            total_entropy = sum(entropy_sources.get(c, 0) for c in components)
            self.logger.debug("Calculated entropy: %d bits", total_entropy)
            
            if total_entropy < MIN_ENTROPY_BITS:
                self.logger.critical(
                    "Insufficient client ID entropy: %d < %d",
                    total_entropy, MIN_ENTROPY_BITS
                )
                raise SecurityError(f"Insufficient client ID entropy: {total_entropy} bits")

            if 'rand4' in components:
                self.logger.warning(
                    "rand4 provides insufficient entropy for client IDs. "
                    "Consider using rand8 or higher."
                )

        valid_order_types = set(self.order_type_mapping.keys())
        defined_params = set(self.exchange_cfg['order_params'].keys())
        
        if undefined_types := (defined_params - valid_order_types):
            self.logger.error(
                "Undefined order types in order_params: %s",
                ', '.join(undefined_types)
            )
            raise ValueError(f"Undefined order types: {', '.join(undefined_types)}")

        duration = time.perf_counter() - start_time
        self.logger.debug("Client ID validation completed in %.4f seconds", duration)

    def get_exchange_order_type(self, internal_type: str) -> str:
        self.logger.debug(
            "Converting internal order type '%s' to exchange-specific type",
            internal_type
        )
        
        try:
            exchange_type = self.order_type_mapping[internal_type]
            self.logger.debug(
                "Mapped internal type '%s' to exchange type '%s'",
                internal_type, exchange_type
            )
            return exchange_type
        except KeyError:
            self.logger.error(
                "Unsupported order type: %s. Valid types: %s",
                internal_type, list(self.order_type_mapping.keys())
            )
            raise

    def format_symbol(self, pair):
        self.logger.debug("Formatting symbol for pair: %s", pair)
        
        fmt = self.exchange_cfg['symbol_format']
        parts = pair.split('_')
        
        if fmt['components'] == ["base", "quote"]:
            base, quote = parts[0], parts[1]
        else:
            quote, base = parts[0], parts[1]
            
        symbol = f"{base}{fmt['delimiter']}{quote}"
        formatted = symbol.upper() if fmt['case'] == "upper" else symbol.lower()
        
        self.logger.debug(
            "Formatted symbol: %s => %s (delimiter: '%s', case: %s)",
            pair, formatted, fmt['delimiter'], fmt['case']
        )
        return formatted

    def parse_message(self, message, message_type):
        self.logger.debug(
            "Parsing %s message: %.200s...",
            message_type, message
        )
        
        try:
            data = json.loads(message)
            if message_type == "ticker":
                event_type_cfg = self.exchange_cfg['channels']['ticker']
                if 'event_type' in event_type_cfg and 'event_path' in event_type_cfg:
                    event = self._extract_value(data, 'ticker.event_path')
                    if event != event_type_cfg['event_type']:
                        self.logger.debug(
                            "Ignoring %s event: %s (expected %s)",
                            message_type, event, event_type_cfg['event_type']
                        )
                        return None

                expr = parse(self.exchange_cfg['channels']['ticker']['path'])
                result = float(next(expr.find(data)).value)
                self.logger.debug("Extracted ticker price: %.4f", result)
                return result
            
            elif message_type == "order":
                parsed = {
                    'status': self._extract_value(data, 'orders.status_path'),
                    'symbol': self._extract_value(data, 'orders.symbol_path'),
                    'client_order_id': self._extract_value(data, 'orders.client_id_path'),
                    'filled': self._extract_value(data, 'orders.filled_path'),
                    'amount': self._extract_value(data, 'orders.amount_path'),
                    'event': self._extract_value(data, 'orders.event_path'),
                }
                self.logger.debug(
                    "Parsed order message: %s",
                    {k: v for k, v in parsed.items() if not k.endswith('_path')}
                )
                return parsed
                
            return None
        except Exception as e:
            self.logger.error(
                "Parse error in %s message: %s\nMessage: %.200s",
                message_type, str(e), message,
                exc_info=self.logger.isEnabledFor(logging.DEBUG)
            )
            return None

    def create_auth_payload(self, params_str):
        self.logger.debug("Creating auth payload for params: %s", params_str)
        
        auth_type = self.exchange_cfg['api']['auth_type']
        secret = self.exchange_cfg['api']['secret'].encode()
        
        try:
            if auth_type == "HMAC_SHA512":
                signature = hmac.new(secret, params_str.encode(), hashlib.sha512).hexdigest()
            elif auth_type == "HMAC_SHA256":
                signature = hmac.new(secret, params_str.encode(), hashlib.sha256).hexdigest()
            else:
                raise ValueError(f"Unsupported auth type: {auth_type}")
            
            self.logger.debug("Generated %s signature: %.10s...", auth_type, signature)
            return {
                self.exchange_cfg['message_templates']['auth']['key_header']: self.exchange_cfg['api']['key'],
                self.exchange_cfg['message_templates']['auth']['sign_header']: signature
            }
        except Exception as e:
            self.logger.error("Auth payload creation failed: %s", str(e))
            raise

    def _get_order_parameters(self, order_type: str, params: Dict) -> Dict:
        self.logger.debug(
            "Generating order parameters for %s order: %s",
            order_type, {k: v for k, v in params.items() if k != 'side'}
        )
        
        try:
            exchange_type = self.get_exchange_order_type(order_type)
            cfg = self.exchange_cfg['order_params'][exchange_type]
            
            if order_type == 'market':
                result = {cfg['amount_key']: str(params['amount'])}
                self.logger.debug("Market order parameters: %s", result)
                return result
            
            base_params = {
                cfg['trigger_key']: str(params['trigger']),
                cfg['limit_key']: str(params['limit']),
                cfg['amount_key']: str(params['amount']),
            }
            
            if params.get('side', '').lower() == 'buy' and order_type != 'market':
                base_params["timeInForce"] = cfg.get('time_in_force', 'IOC')
                
            self.logger.debug(
                "Stop limit order parameters: %s (timeInForce: %s)",
                base_params, base_params.get("timeInForce", "N/A")
            )
            return base_params
        except Exception as e:
            self.logger.error(
                "Failed to generate order parameters: %s",
                str(e),
                exc_info=self.logger.isEnabledFor(logging.DEBUG)
            )
            raise

    def _extract_value(self, data, config_path):
        self.logger.debug("Extracting value using config path: %s", config_path)
        
        try:
            json_path = self.exchange_cfg['channels'][config_path]
            result = next(parse(json_path).find(data)).value
            self.logger.debug("Extracted value: %s", result)
            return result
        except Exception as e:
            self.logger.error(
                "Value extraction failed for path '%s': %s",
                config_path, str(e),
                exc_info=self.logger.isEnabledFor(logging.DEBUG)
            )
            raise
