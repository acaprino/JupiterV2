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
from notifiers.mock_market_state_notifier import MockMarketStateNotifier
from notifiers.new_tick_notifier import TickNotifier
from strategies.adrastea import Adrastea
from brokers.mt5_broker import MT5Broker
from utils.config import ConfigReader
from utils.logger import log_init, log_info, log_error

from utils.async_executor import executor
from utils.mongo_db import MongoDB

TEST_MODE = False


async def main(config_file: str):
    """
    Main function that starts the asynchronous trading bot.
    """
    config = ConfigReader(config_file)

    # Configure logging
    warnings.filterwarnings('ignore', category=FutureWarning)
    log_init(config.get_bot_name(), config.get_bot_version(), config.get_bot_logging_level())

    mongo_db = MongoDB(config.get_mongo_host(), config.get_mongo_port())

    if not mongo_db.test_connection():
        log_error("MongoDB connection failed. Exiting...")
        return

    # Initialize the broker
    broker: BrokerAPI = MT5Broker(config)

    # Create the lock to synchronize executions
    execution_lock = asyncio.Lock()

    # Initialize the MarketStateNotifier
    tick_notifier = TickNotifier(timeframe=config.get_timeframe(), execution_lock=execution_lock)

    if TEST_MODE:
        market_state_notifier = MockMarketStateNotifier(broker, config.get_symbol(), execution_lock)
    else:
        market_state_notifier = MarketStateNotifier(broker, config.get_symbol(), execution_lock)

    economic_event_notifier = EconomicEventNotifier(broker, symbol=config.get_symbol(), execution_lock=execution_lock)
    closed_deals_notifier = ClosedPositionNotifier(broker, symbol=config.get_symbol(), magic_number=config.get_bot_magic_number(), execution_lock=execution_lock)

    # Instantiate the strategy
    strategy = Adrastea(broker, config, execution_lock)

    # Register event handlers
    tick_notifier.register_on_new_tick(strategy.on_new_tick)
    market_state_notifier.register_on_market_status_change(strategy.on_market_status_change)
    economic_event_notifier.register_on_economic_event(strategy.on_economic_event)
    closed_deals_notifier.register_on_deal_status_notifier(strategy.on_deal_closed)

    # Execute the strategy bootstrap method
    if not TEST_MODE:
        await asyncio.create_task(strategy.initialize())
    await market_state_notifier.start()
    await tick_notifier.start()
    await economic_event_notifier.start()
    await closed_deals_notifier.start()

    try:
        # Keep the program running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        log_info("Keyboard interruption detected. Stopping the bot...")
    finally:
        # Stop the providers and close the broker connection
        await strategy.shutdown()
        await market_state_notifier.stop()
        await tick_notifier.stop()
        await economic_event_notifier.stop()
        await closed_deals_notifier.stop()
        broker.shutdown()
        log_info("Program terminated.")

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
    loop.run_until_complete(main(config_file_param))
