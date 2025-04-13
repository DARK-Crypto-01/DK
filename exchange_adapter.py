import hashlib
import hmac
import json
import logging
from jsonpath_ng import parse
from typing import Dict

class ExchangeAdapter:
    def __init__(self, config, exchange_name):
        self.exchange_cfg = config['exchanges'][exchange_name]
        self.strategy_cfg = config['strategy']
        self.logger = logging.getLogger("ExchangeAdapter")
        self.validate_client_id_config()

    def validate_client_id_config(self):
        """Validate client ID configuration structure"""
        if 'client_id' not in self.exchange_cfg:
            return
            
        id_cfg = self.exchange_cfg['client_id']
        valid_patterns = ['uuid', 'timestamp']
        
        if 'pattern' not in id_cfg:
            raise ValueError("Missing 'pattern' in client_id configuration")
            
        if id_cfg['pattern'] not in valid_patterns:
            raise ValueError(
                f"Invalid client_id pattern '{id_cfg['pattern']}'. "
                f"Allowed values: {valid_patterns}"
            )
            
        if id_cfg['pattern'] == 'timestamp' and 'format' not in id_cfg:
            self.logger.error("Timestamp pattern requires format definition")
            raise ValueError("Missing format for timestamp client_id pattern")

    def format_symbol(self, pair):
        fmt = self.exchange_cfg['symbol_format']
        parts = pair.split('_')
        
        if fmt['components'] == ["base", "quote"]:
            base, quote = parts[0], parts[1]
        else:
            quote, base = parts[0], parts[1]
            
        symbol = f"{base}{fmt['delimiter']}{quote}"
        return symbol.upper() if fmt['case'] == "upper" else symbol.lower()

    def parse_message(self, message, message_type):
        try:
            data = json.loads(message)
            if message_type == "ticker":
                # First validate event type if configured
                event_type_cfg = self.exchange_cfg['channels']['ticker']
                if 'event_type' in event_type_cfg and 'event_path' in event_type_cfg:
                    event = self._extract_value(data, 'ticker.event_path')
                    if event != event_type_cfg['event_type']:
                        self.logger.debug(f"Ignoring {message_type} event: {event}")
                        return None

                # Proceed with price extraction
                expr = parse(self.exchange_cfg['channels']['ticker']['path'])
                return float(next(expr.find(data)).value
            
            elif message_type == "order":
                return {
                    'status': self._extract_value(data, 'orders.status_path'),
                    'symbol': self._extract_value(data, 'orders.symbol_path'),
                    'client_order_id': self._extract_value(data, 'orders.client_id_path'),
                    'filled': self._extract_value(data, 'orders.filled_path'),
                    'amount': self._extract_value(data, 'orders.amount_path'),
                    'event': self._extract_value(data, 'orders.event_path'),
                }
            return None
        except Exception as e:
            self.logger.error(f"Parse error: {str(e)}")
            return None

    def create_auth_payload(self, params_str):
        auth_type = self.exchange_cfg['api']['auth_type']
        secret = self.exchange_cfg['api']['secret'].encode()
        
        if auth_type == "HMAC_SHA512":
            signature = hmac.new(secret, params_str.encode(), hashlib.sha512).hexdigest()
        elif auth_type == "HMAC_SHA256":
            signature = hmac.new(secret, params_str.encode(), hashlib.sha256).hexdigest()
        else:
            raise ValueError(f"Unsupported auth type: {auth_type}")

        return {
            self.exchange_cfg['message_templates']['auth']['key_header']: self.exchange_cfg['api']['key'],
            self.exchange_cfg['message_templates']['auth']['sign_header']: signature
        }

    def _get_order_parameters(self, order_type: str, params: Dict) -> Dict:
        """Map order parameters to exchange-specific names with IOC only for buys"""
        cfg = self.exchange_cfg['order_params'][order_type]
        
        if order_type == 'market':
            return {
                cfg['amount_key']: str(params['amount'])
            }
        
        base_params = {
            cfg['trigger_key']: str(params['trigger']),
            cfg['limit_key']: str(params['limit']),
            cfg['amount_key']: str(params['amount']),
        }
        
        # Add time_in_force only for buy orders and non-market orders
        if params.get('side', '').lower() == 'buy' and order_type != 'market':
            base_params["timeInForce"] = cfg.get('time_in_force', 'IOC')
            
        return base_params

    def _extract_value(self, data, config_path):
        json_path = self.exchange_cfg['channels'][config_path]
        return next(parse(json_path).find(data)).value
