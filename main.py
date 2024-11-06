# main.py
import argparse
import asyncio
import sys
import warnings

from brokers.broker_interface import BrokerAPI
from providers.candle_provider import CandleProvider
from providers.deal_status_notifier import ClosedDealsNotifier
from providers.economic_event_notifier import EconomicEventNotifier
from providers.market_state_notifier import MarketStateNotifier
from strategies.adrastea import Adrastea
from brokers.mt5_broker import MT5Broker
from utils.config import ConfigReader
from utils.logger import log_init, log_info

from utils.async_executor import executor
from utils.mongo_db import MongoDB


async def main(config_file: str):
    """
    Main function that starts the asynchronous trading bot.
    """
    config = ConfigReader(config_file)
    mongoDB = MongoDB(config_file)

    # Configure logging
    warnings.filterwarnings('ignore', category=FutureWarning)
    log_init(config.get_bot_name(), config.get_bot_version(), config.get_bot_logging_level())

    # Initialize the broker
    broker: BrokerAPI = MT5Broker(config)

    # Create the lock to synchronize executions
    execution_lock = asyncio.Lock()

    # Initialize the MarketStateNotifier
    market_state_notifier = MarketStateNotifier(broker, config.get_symbol(), execution_lock)
    candle_provider = CandleProvider(broker, symbol=config.get_symbol(), timeframe=config.get_timeframe(), execution_lock=execution_lock)
    economic_event_notifier = EconomicEventNotifier(broker, config.get_symbol(), execution_lock)
    closed_deals_notifier = ClosedDealsNotifier(broker, config.get_symbol(), execution_lock)

    # Instantiate the strategy
    strategy = Adrastea(broker, config, market_state_notifier, candle_provider)

    # Register event handlers
    candle_provider.register_on_new_candle(strategy.on_new_candle)
    market_state_notifier.register_on_market_status_change(strategy.on_market_status_change)
    economic_event_notifier.register_on_economic_event(strategy.on_economic_event)
    closed_deals_notifier.register_on_deal_status_notifier(strategy.on_deal_closed)

    # Execute the strategy bootstrap method
    await strategy.bootstrap()
    await market_state_notifier.start()
    await candle_provider.start()
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
        await candle_provider.stop()
        await market_state_notifier.stop()
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

    asyncio.run(main(config_file_param))