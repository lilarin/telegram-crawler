import asyncio
import os

from config import logger, config
from app.pipelines.telegram_crawler import TelegramCrawler
from app.pipelines.tgstat_scraper import TGStatScraper


async def main():
    logger.info("Scraping TGStat for channels...")
    crawl_telegram = True
    scrape_tgstat = False

    categories = [
        "politics",
        "blogs",
        "news",
        "economics"
    ]

    if scrape_tgstat:
        scraper = TGStatScraper()
        channels_by_category = scraper.run(categories)

        if not channels_by_category:
            return
    else:
        if not os.path.exists(config.SCRAPPED_CHANNELS_FILE):
            return

    logger.info("Process the collected channels with Telegram API...")

    if crawl_telegram:
        crawler = TelegramCrawler()
        await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
