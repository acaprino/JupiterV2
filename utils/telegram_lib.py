import asyncio
import threading
from queue import Queue
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler

from utils.bot_logger import BotLogger
from utils.error_handler import exception_handler


class TelegramBotWrapper:
    _instance = None  # Class attribute to store the singleton instance

    def __new__(cls, *args, **kwargs):
        # Implement singleton pattern
        if cls._instance is None:
            cls._instance = super(TelegramBotWrapper, cls).__new__(cls)
        return cls._instance

    def __init__(self, token, bot_name):
        if hasattr(self, '_initialized') and self._initialized:
            return  # Prevent reinitialization
        self.token = token
        self.application = None
        self.bot_thread = None
        self.loop = None
        self.ready_event = threading.Event()
        self.logger = BotLogger.get_logger(bot_name)
        self.message_queue = Queue()
        self._initialized = True  # Mark instance as initialized

    def start(self):
        self.bot_thread = threading.Thread(target=self._run_bot)
        self.bot_thread.start()
        self.ready_event.wait()

    def _run_bot(self):
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:  # No loop found
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self.application = Application.builder().token(self.token).build()

        # Initialize and signal that the bot is ready
        self.loop.run_until_complete(self.application.initialize())
        self.ready_event.set()

        # Start the message processing task
        asyncio.run_coroutine_threadsafe(self._process_queue(), self.loop)

        # Start polling and keep the bot running
        self.loop.run_until_complete(self.application.run_polling())

    async def _process_queue(self):
        """Process the message queue to send messages in order."""
        while True:
            chat_id, text, reply_markup = self.message_queue.get()
            try:
                await self._send_message(chat_id, text, reply_markup)
            except Exception as e:
                self.logger.error(f"Failed to send message: {e}")
            self.message_queue.task_done()

    def send_message(self, chat_id, text, reply_markup=None):
        # Ensure the bot is fully initialized before queuing a message
        if self.ready_event.is_set():
            self.message_queue.put((chat_id, text, reply_markup))

    @exception_handler
    async def _send_message(self, chat_id, text, reply_markup=None):
        await self.application.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

    def stop(self):
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self.bot_thread:
            self.bot_thread.join()

    def add_command_callback_handler(self, handler):
        if self.application:
            self.application.add_handler(CallbackQueryHandler(handler))
