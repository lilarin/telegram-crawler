import pickle
import time
from typing import Dict, List, Tuple

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait

from app.config import logger, config
from app.core.database import async_session
from app.repositories.category_repository import CategoryRepository
from app.repositories.tgstat_repository import TGStatRepository


class TGStatScraper:
    def __init__(self):
        ua = UserAgent()
        user_agent = ua.random

        self.options = Options()
        self.options.add_argument("--start-maximized")
        self.options.add_argument("window-size=1280,720")
        self.options.add_argument(f"user-agent={user_agent}")
        self.options.add_argument("--headless")
        self.driver = None
        self.channels_by_category: Dict[str, List[str]] = {}

    async def load_channels_from_db(self):
        async with async_session() as session:
            repo = CategoryRepository(session)
            self.channels_by_category = await repo.get_all_channels_by_category()
            logger.info(f"Loaded {len(self.channels_by_category)} categories from database")

            return self.channels_by_category

    def initialize_driver(self):
        self.driver = webdriver.Chrome(options=self.options)

    def scroll_to_bottom(self):
        show_more_button_xpath = "//button[contains(text(), 'Показать больше') or contains(text(), 'Показати більше')]"

        while True:
            try:
                show_more_button = WebDriverWait(self.driver, 3).until(
                    expected_conditions.element_to_be_clickable(
                        (By.XPATH, show_more_button_xpath)
                    )
                )
                self.driver.execute_script("arguments[0].click();", show_more_button)
                time.sleep(1)
            except TimeoutException:
                break
            except Exception as e:
                logger.error(f"Error while scrolling: {type(e)}")
                break

    @staticmethod
    def extract_channel_username(url):
        if "?" in url:
            url = url.split("?")[0]

        username = url.strip("/").split("/")[-1]

        if not username or "#" in username or len(username) < 2:
            return None, False

        if username.startswith("@"):
            return username[1:], True

        return username, False

    def collect_channel_detail_urls(self):
        detail_urls = []
        selector = "//div[contains(@class, 'card card-body peer-item-box')]"

        cards = self.driver.find_elements(By.XPATH, selector)

        for i, card in enumerate(cards):
            try:
                links = card.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute("href")
                    if href and href.startswith("http") and href not in detail_urls:
                        detail_urls.append(href)
            except StaleElementReferenceException:
                logger.error(f"Element unreachable, skipping..")
                continue
            except Exception as e:
                logger.error(f"Error processing card #{i + 1}: {e}")
                continue

        return detail_urls

    async def save_to_db(self, category_name: str, processed_urls: List[str]):
        logger.info(
            f"Saving to database: category={category_name}, channels={processed_urls}"
        )
        async with async_session() as session:
            try:
                repo = TGStatRepository(session)
                success = await repo.save_channels_for_category(
                    category_name, processed_urls
                )

                if success:
                    if category_name not in self.channels_by_category:
                        self.channels_by_category[category_name] = []

                    for url in processed_urls:
                        if url not in self.channels_by_category[category_name]:
                            self.channels_by_category[category_name].append(url)

                return success

            except Exception as e:
                logger.error(f"Error saving to database: {str(e)}")
                return False

    def process_channel_urls(
        self, channel_urls: List[str], category_name: str
    ) -> List[str]:
        if category_name not in self.channels_by_category:
            self.channels_by_category[category_name] = []

        processed_urls = []
        for url in channel_urls:
            username, is_public = self.extract_channel_username(url)

            if username:
                if is_public:
                    telegram_url = f"https://t.me/{username}"
                else:
                    telegram_url = f"https://t.me/joinchat/{username}"

                url_exists = False
                for urls in self.channels_by_category.values():
                    if telegram_url in urls:
                        url_exists = True
                        break

                if not url_exists:
                    processed_urls.append(telegram_url)

        return processed_urls

    def scrape_category(self, url: str) -> Tuple[str, List[str]]:
        try:
            category_name = url.split("/")[-1]
            logger.info(f"Scraping category: {category_name}...")

            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                expected_conditions.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)

            self.scroll_to_bottom()

            channel_urls = self.collect_channel_detail_urls()
            processed_urls = self.process_channel_urls(channel_urls, category_name)

            return category_name, processed_urls
        except Exception as e:
            logger.error(f"Error scraping category: {e}")
            return "", []

    async def run(self, categories: List[str]):
        try:
            await self.load_channels_from_db()

            self.initialize_driver()

            self.driver.get(config.BASE_URL)

            cookies = pickle.load(open(config.COOKIES_FILE, "rb"))
            for cookie in cookies:
                self.driver.add_cookie(cookie)

            for category in categories:
                category_name, processed_urls = self.scrape_category(
                    f"{config.BASE_URL}/{category}"
                )

                if processed_urls:
                    await self.save_to_db(category_name, processed_urls)

            return self.channels_by_category

        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            return None
        finally:
            if self.driver:
                self.driver.quit()
