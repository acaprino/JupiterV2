# main.py
import argparse
import asyncio
import sys
import warnings

from brokers.broker_interface import BrokerAPI
from providers.candle_provider import CandleProvider
from providers.economic_event_notifier import EconomicEventNotifier
from providers.market_state_notifier import MarketStateNotifier
from strategies.adrastea import Adrastea
from brokers.mt5_broker import MT5Broker
from utils.config import ConfigReader
from utils.logger import log_init, log_info

from utils.async_executor import executor

async def main(config_file_param: str):
    """
    Funzione principale che avvia il bot di trading asincrono.
    """
    config = ConfigReader(config_file_param)

    # Configura il logging
    warnings.filterwarnings('ignore', category=FutureWarning)
    log_init(config.get_bot_name(), config.get_bot_version(), config.get_bot_logging_level())

    # Inizializza il broker
    broker: BrokerAPI = MT5Broker(config)

    # Crea il lock per sincronizzare le esecuzioni
    execution_lock = asyncio.Lock()

    # Inizializza il MarketStateNotifier
    market_state_notifier = MarketStateNotifier(broker, config.get_symbol(), execution_lock)
    await market_state_notifier.start()

    # Inizializza il CandleProvider con lo stesso simbolo
    candle_provider = CandleProvider(broker, symbol=config.get_symbol(), timeframe=config.get_timeframe(), execution_lock=execution_lock)
    await candle_provider.start()

    # Inizializza il EconomicEventNotifier con lo stesso simbolo
    economic_event_notifier = EconomicEventNotifier(broker, config.get_symbol(), execution_lock)
    await economic_event_notifier.start()

    # Istanzia la strategia
    strategy = Adrastea(broker, config, market_state_notifier, candle_provider)

    # Registra gli handler degli eventi
    candle_provider.register_on_new_candle(strategy.on_new_candle)
    market_state_notifier.register_on_market_status_change(strategy.on_market_status_change)
    economic_event_notifier.register_on_economic_event(strategy.on_economic_event)

    # Esegui il metodo di bootstrap della strategia
    await strategy.bootstrap()

    try:
        # Mantieni il programma in esecuzione
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        log_info("Interruzione da tastiera rilevata. Arresto del bot...")
    finally:
        # Arresta i provider e chiudi la connessione broker
        await candle_provider.stop()
        await market_state_notifier.stop()
        await economic_event_notifier.stop()
        broker.shutdown()
        log_info("Programma terminato.")

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
