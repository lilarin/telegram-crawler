import asyncio
import os

from app.config import logger, config
from app.services.telegram_service import TelegramCrawler
from app.services.tgstat_service import TGStatScraper
from app.core.database import init_db


async def main():
    # Initialize database
    await init_db()
    
    logger.info("Scraping TGStat for channels...")
    scrape_tgstat = True
    crawl_telegram = False

    categories = [
        # "politics",
        # "blogs",
        # "news",
        # "economics"
        "handmade"
    ]

    if scrape_tgstat:
        scraper = TGStatScraper()
        await scraper.run(categories)

    logger.info("Process the collected channels with Telegram API...")

    if crawl_telegram:
        crawler = TelegramCrawler()
        await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
