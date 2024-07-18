from starlette.config import Config
from telegram import Bot
import asyncio
import os
import sys
from starlette.config import Config

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
# 환경 변수 파일 관리
config = Config(".env")

TELEGRAM_BOT_TOKEN: str = config.get('TELEGRAM_BOT_TOKEN')
CHAT_ID: int = config.get('CHAT_ID')
CHAT_ID2: int = config.get('CHAT_ID2')

# Error Message
async def error_message(text: str) -> None:
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)        
        await bot.send_message(chat_id=CHAT_ID, text=text)
    except Exception as e:
        pass


# Error Message
async def error_message2(text: str) -> None:
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)        
        await bot.send_message(chat_id=CHAT_ID2, text=text)
    except Exception as e:
        pass