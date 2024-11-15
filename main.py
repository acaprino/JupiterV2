# main.py
import argparse
import asyncio
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor

from brokers.broker_interface import BrokerAPI
from notifiers.closed_positions_notifier import ClosedPositionNotifier
from notifiers.economic_event_notifier import EconomicEventNotifier
from notifiers.market_state_notifier import MarketStateNotifier
from notifiers.new_tick_notifier import TickNotifier
from strategies.adrastea import Adrastea
from brokers.mt5_broker import MT5Broker
from utils.config import ConfigReader, TradingConfiguration

from utils.async_executor import executor
from utils.bot_logger import BotLogger
from utils.error_handler import exception_handler
from utils.mongo_db import MongoDB


@exception_handler
async def main(config: ConfigReader, trading_config: TradingConfiguration):
    """
    Main function that starts the asynchronous trading bot.
    """

    # Configure logging
    bot_name = f"{config.get_bot_name()}_{trading_config.get_symbol()}_{trading_config.get_timeframe().name}_{trading_config.get_trading_direction().name}"
    logger = BotLogger.get_logger(name=f"{bot_name}", level=config.get_bot_logging_level().upper())
    warnings.filterwarnings('ignore', category=FutureWarning)
    mongo_db = MongoDB(bot_name=bot_name, host=config.get_mongo_host(), port=config.get_mongo_port())

    if not mongo_db.test_connection():
        logger.error("MongoDB connection failed. Exiting...")
        print("MongoDB connection failed. Exiting...")
        return

    # Initialize the broker
    broker: BrokerAPI = MT5Broker(bot_name=bot_name, account=config.get_broker_account(), password=config.get_broker_password(), server=config.get_broker_server(), path=config.get_broker_mt5_path())

    # Create the lock to synchronize executions
    execution_lock = asyncio.Lock()

    # Initialize the MarketStateNotifier
    tick_notifier = TickNotifier(bot_name=bot_name, timeframe=trading_config.get_timeframe(), execution_lock=execution_lock)

    market_state_notifier = MarketStateNotifier(bot_name=bot_name, broker=broker, symbol=trading_config.get_symbol(), execution_lock=execution_lock)
    economic_event_notifier = EconomicEventNotifier(bot_name=bot_name, broker=broker, symbol=trading_config.get_symbol(), execution_lock=execution_lock)
    closed_deals_notifier = ClosedPositionNotifier(bot_name=bot_name, broker=broker, symbol=trading_config.get_symbol(), magic_number=config.get_bot_magic_number(), execution_lock=execution_lock)

    # Instantiate the strategy
    strategy = Adrastea(bot_name=bot_name, broker=broker, config=config, trading_config=trading_config, execution_lock=execution_lock)

    # Register event handlers
    tick_notifier.register_on_new_tick(strategy.on_new_tick)
    market_state_notifier.register_on_market_status_change(strategy.on_market_status_change)
    economic_event_notifier.register_on_economic_event(strategy.on_economic_event)
    closed_deals_notifier.register_on_deal_status_notifier(strategy.on_deal_closed)

    # Execute the strategy bootstrap method

    await strategy.start()
    asyncio.create_task(strategy.initialize())
    await market_state_notifier.start()
    await tick_notifier.start()
    await economic_event_notifier.start()
    await closed_deals_notifier.start()

    try:
        # Keep the program running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interruption detected. Stopping the bot...")
    finally:
        # Stop the providers and close the broker connection
        await strategy.shutdown()
        await market_state_notifier.stop()
        await tick_notifier.stop()
        await economic_event_notifier.stop()
        await closed_deals_notifier.stop()
        broker.shutdown()
        logger.info("Program terminated.")

        executor.shutdown()


if __name__ == "__main__":
    sys.stdin.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8')

    # Read command-line parameters
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Bot launcher script.')
    parser.add_argument('config_file', nargs='?', default='config.json', help='Path to the configuration file.')

    # Parse the command-line arguments
    args = parser.parse_args()

    config_file_param = args.config_file

    print(f"Config file: {config_file_param}")

    executor = ThreadPoolExecutor(max_workers=5)
    loop = asyncio.new_event_loop()
    loop.set_default_executor(executor)
    asyncio.set_event_loop(loop)

    config = ConfigReader.load_config(config_file_param=config_file_param)

    trading_configs = config.get_trading_configurations()

    for trading_config in trading_configs:
        loop.run_until_complete(main(config, trading_config))
