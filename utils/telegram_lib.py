import asyncio

from pyparsing import Optional
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler

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
        self.loop = asyncio.get_event_loop()
        self.message_queue = asyncio.Queue()
        self.logger = BotLogger.get_logger(bot_name)
        self._initialized = True  # Mark instance as initialized

    async def start(self):
        """Start the bot application and the message processing loop."""
        self.application = Application.builder().token(self.token).build()

        # Initialize the bot
        await self.application.initialize()

        # Start the message processing task
        asyncio.create_task(self._process_queue())

        # Start polling
        await self.application.run_polling()

    async def _process_queue(self):
        """Process the message queue to send messages in order."""
        while True:
            chat_id, text, reply_markup = await self.message_queue.get()
            try:
                await self._send_message(chat_id, text, reply_markup)
            except Exception as e:
                self.logger.error(f"Failed to send message: {e}")
            finally:
                self.message_queue.task_done()

    def send_message(self, chat_id, text, reply_markup=None):
        """Enqueue a message to be sent."""
        asyncio.run_coroutine_threadsafe(
            self.message_queue.put((chat_id, text, reply_markup)),
            self.loop
        )

    @exception_handler
    async def _send_message(self, chat_id, text, reply_markup=None):
        """Send a message asynchronously."""
        await self.application.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )

    async def stop(self):
        """Stop the bot and clean up resources."""
        await self.application.shutdown()
        await self.application.stop()

    def add_command_callback_handler(self, command: str, handler):
        """
        Add a command handler to the bot's application.

        :param command: The command to trigger this handler (without '/').
        :param handler: The async function to handle the command.
        """
        if self.application:
            self.application.add_handler(CommandHandler(command, handler))
        else:
            self.logger.error("Application not initialized. Cannot add command handler.")

    def add_callback_query_handler(self, handler, pattern: str = None):
        """
        Add a callback query handler to the bot's application.

        :param handler: The async function to handle the callback.
        :param pattern: (Optional) Regex pattern to filter callback data. Default is None.
        """
        if self.application:
            self.application.add_handler(CallbackQueryHandler(handler, pattern=pattern))
        else:
            self.logger.error("Application not initialized. Cannot add callback query handler.")
