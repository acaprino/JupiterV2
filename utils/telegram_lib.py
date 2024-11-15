import asyncio
import threading
from queue import Queue
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler

from utils.bot_logger import BotLogger
from utils.error_handler import exception_handler


class TelegramBotWrapper:
    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, token, worker_id, *args, **kwargs):
        with cls._lock:
            if token not in cls._instances:
                cls._instances[token] = super(TelegramBotWrapper, cls).__new__(cls)
        return cls._instances[token]

    def __init__(self, token, worker_id):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self.token = token
        self.worker_id = worker_id
        self.application = None
        self.loop = None
        self.ready_event = threading.Event()
        self.logger = BotLogger.get_logger(worker_id)
        self.message_queue = Queue()
        self._thread_lock = threading.Lock()
        self._is_running = False
        self.bot_thread = None
        self._initialized = True

    def start(self):
        if self._is_running:
            self.logger.warning(f"Bot {self.worker_id} is already running.")
            return

        self._is_running = True
        self.bot_thread = threading.Thread(target=self._run_bot, daemon=True)
        self.bot_thread.start()

    def _run_bot(self):
        try:
            try:
                self.loop = asyncio.get_running_loop()
                self.logger.info("Using existing running event loop.")
            except RuntimeError:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                self.logger.info("Created and set a new event loop.")

            self.application = Application.builder().token(self.token).build()

            self.loop.run_until_complete(self.application.initialize())
            self.ready_event.set()
            self.loop.run_until_complete(self.application.run_polling())
        except Exception as e:
            self.logger.error(f"Error in bot thread: {e}")
        finally:
            self._is_running = False

    def send_message(self, chat_id, text, reply_markup=None):
        with self._thread_lock:
            if self.ready_event.is_set():
                self.message_queue.put((chat_id, text, reply_markup))

    async def _process_queue(self):
        while True:
            chat_id, text, reply_markup = self.message_queue.get()
            try:
                await self._send_message(chat_id, text, reply_markup)
            except Exception as e:
                self.logger.error(f"Failed to send message: {e}")
            self.message_queue.task_done()

    @exception_handler
    async def _send_message(self, chat_id, text, reply_markup=None):
        await self.application.bot.send_message(
            chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
        )

    def stop(self):
        if not self._is_running:
            self.logger.warning(f"Bot {self.worker_id} is not running.")
            return

        if self.application:
            self.loop.call_soon_threadsafe(self.application.shutdown)
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)

        if self.bot_thread and self.bot_thread.is_alive():
            self.bot_thread.join()

        self._is_running = False

    def add_command_callback_handler(self, command: str, handler):
        if not self.ready_event.wait(timeout=10):
            self.logger.error("Timeout while waiting for the application to be initialized.")
            return

        with self._thread_lock:
            if self.application:
                try:
                    self.application.add_handler(CommandHandler(command, handler))
                    self.logger.info(f"Added command handler for '{command}'.")
                except Exception as e:
                    self.logger.error(f"Failed to add command handler for '{command}': {e}")
            else:
                self.logger.error("Application not initialized. Cannot add command handler.")

    def add_callback_query_handler(self, handler, pattern: str = None):
        if not self.ready_event.wait(timeout=10):
            self.logger.error("Timeout while waiting for the application to be initialized.")
            return

        with self._thread_lock:
            if self.application:
                try:
                    self.application.add_handler(CallbackQueryHandler(handler, pattern=pattern))
                    self.logger.info(f"Added callback query handler with pattern '{pattern}'.")
                except Exception as e:
                    self.logger.error(f"Failed to add callback query handler: {e}")
            else:
                self.logger.error("Application not initialized. Cannot add callback query handler.")

