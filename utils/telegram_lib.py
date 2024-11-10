import asyncio
import threading

from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler


class TelegramBotWrapper:
    _instance = None  # Class attribute to store the singleton instance

    def __new__(cls, *args, **kwargs):
        # Implement singleton pattern
        if cls._instance is None:
            cls._instance = super(TelegramBotWrapper, cls).__new__(cls)
        return cls._instance

    def __init__(self, token):
        if hasattr(self, '_initialized') and self._initialized:
            return  # Prevent reinitialization
        self.token = token
        self.application = None
        self.bot_thread = None
        self.loop = None
        self.ready_event = threading.Event()
        self._initialized = True  # Mark instance as initialized

    def start(self):
        self.bot_thread = threading.Thread(target=self._run_bot)
        self.bot_thread.start()
        self.ready_event.wait()

    def _run_bot(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.application = Application.builder().token(self.token).build()

        # Initialize and signal that the bot is ready
        self.loop.run_until_complete(self.application.initialize())
        self.ready_event.set()

        # Start polling and keep the bot running
        self.loop.run_until_complete(self.application.run_polling())

    def send_message(self, chat_id, text, reply_markup=None):
        # Ensure the bot is fully initialized before sending a message
        if self.ready_event.is_set() and self.application:
            asyncio.run_coroutine_threadsafe(
                self._send_message(chat_id, text, reply_markup),
                self.loop
            )

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
