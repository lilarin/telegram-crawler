import asyncio

from app.config import logger
from app.core.database import init_db
from app.services.telegram_service import TelegramCrawler
from app.services.tgstat_service import TGStatScraper


async def main():
    # Initialize database
    await init_db()

    logger.info("Scraping TGStat for channels...")
    scrape_tgstat = False
    crawl_telegram = True

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
        # Pass the same categories we scraped to ensure we process only those
        await crawler.run(categories)


if __name__ == "__main__":
    asyncio.run(main())
