import uvloop
import logging
import sys
import yaml
import asyncio
from datetime import datetime
from trading_strategy import TradingStrategy
from exchange_adapter import ExchangeAdapter

# Initialize module-specific logger
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration with detailed logging"""
    try:
        logger.debug("Attempting to load configuration file")
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            
            logger.info("Config file loaded successfully")
            logger.debug("Validating exchange configuration")
            
            default_exchange = config['strategy']['default_exchange']
            if not config['exchanges'].get(default_exchange):
                logger.error(f"Configured exchange {default_exchange} not found in config")
                raise ValueError(f"Exchange {default_exchange} not found")
                
            logger.debug(f"Exchange configuration validated for {default_exchange}")
            return config
            
    except Exception as e:
        logger.critical(f"Config error: {str(e)}", exc_info=True)
        sys.exit(1)

def setup_logging(config):
    """Configure logging system with detailed initialization"""
    log_cfg = config.get('logging', {})
    if not log_cfg.get('enabled', True):
        logger.warning("Logging is disabled in configuration")
        return
        
    logger.info("Initializing logging system")
    
    handlers = []
    log_level = log_cfg.get('level', 'INFO').upper()
    
    # File handler configuration
    if log_cfg.get('file'):
        from logging.handlers import RotatingFileHandler
        try:
            file_handler = RotatingFileHandler(
                filename=log_cfg['file'],
                maxBytes=log_cfg.get('max_size_mb', 10) * 1024*1024,
                backupCount=log_cfg.get('backups', 3)
            )
            file_handler.setLevel(log_level)
            handlers.append(file_handler)
            logger.debug(f"File logging configured: {log_cfg['file']}")
        except Exception as e:
            logger.error(f"Failed to configure file logging: {str(e)}")

    # Console handler configuration
    if log_cfg.get('console', True):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        handlers.append(console_handler)
        logger.debug("Console logging configured")

    # Apply basic configuration
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=handlers
    )
    
    logger.info(f"Logging system initialized at {log_level} level")

async def async_main():
    """Main async entry point with enhanced logging"""
    logger.info("Application starting")
    start_time = datetime.now()
    
    try:
        # Load and validate configuration
        config = load_config()
        setup_logging(config)
        
        # Initialize exchange components
        exchange_name = config['strategy']['default_exchange']
        logger.info(f"Initializing exchange adapter for {exchange_name}")
        
        adapter = ExchangeAdapter(config, exchange_name)
        logger.debug("Exchange adapter instance created")
        
        # Validate credentials
        logger.debug("Validating exchange credentials")
        if not adapter.validate_credentials():
            logger.error("Exchange credentials validation failed")
            raise ValueError("Invalid exchange credentials")
        logger.info("Exchange credentials validated successfully")
        
        # Initialize trading strategy
        logger.info("Initializing trading strategy")
        strategy = TradingStrategy(config, adapter)
        logger.debug("Trading strategy instance created")
        
        # Start main strategy loop
        logger.info("Starting main strategy loop")
        await strategy.manage_strategy()
        
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received, initiating shutdown")
        shutdown_start = datetime.now()
        await strategy.shutdown()
        logger.info(f"Graceful shutdown completed in {datetime.now() - shutdown_start}")
        
    except Exception as e:
        logger.critical(f"Fatal error occurred: {str(e)}", exc_info=True)
        raise
        
    finally:
        logger.info(f"Application runtime: {datetime.now() - start_time}")
        logger.info("Trading session ended")

if __name__ == "__main__":
    logger.debug("Initializing uvloop event loop")
    uvloop.install()
    asyncio.run(async_main()) 
