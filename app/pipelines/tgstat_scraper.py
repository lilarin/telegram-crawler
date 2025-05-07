import json
import pickle
import time
import os

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from config import logger, config


class TGStatScraper:
    def __init__(self):
        ua = UserAgent()
        user_agent = ua.random

        self.options = Options()
        self.options.add_argument("--start-maximized")
        self.options.add_argument("window-size=1280,720")
        self.options.add_argument(f'user-agent={user_agent}')
        self.options.add_argument("--headless")
        self.driver = webdriver.Chrome(options=self.options)
        self.channels_by_category = {}

        if os.path.exists(config.SCRAPPED_CHANNELS_FILE):
            with open(config.SCRAPPED_CHANNELS_FILE, "r", encoding="utf-8") as f:
                self.channels_by_category = json.load(f)

    @staticmethod
    def wait_for_login(timeout=10):
        logger.info(f"Waiting {timeout} to log in manually...")
        time.sleep(timeout)

    def scroll_to_bottom(self):
        show_more_button_xpath = "//button[contains(text(), 'Показать больше') or contains(text(), 'Показати більше')]"

        while True:
            try:
                show_more_button = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, show_more_button_xpath))
                )
                self.driver.execute_script("arguments[0].click();", show_more_button)

            except TimeoutException:
                break
            except Exception as e:
                logger.error(f"Error while scrolling: {type(e)}")
            finally:
                time.sleep(1)

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

    def process_channel_urls(self, channel_urls, category_name, output_file):
        if category_name not in self.channels_by_category:
            self.channels_by_category[category_name] = []

        for i, url in enumerate(channel_urls):
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
                    self.channels_by_category[category_name].append(telegram_url)
                    self.save_results(output_file)

    def scrape_category(self, url, output_file):
        try:
            category_name = url.split('/')[-1]

            self.driver.get(url)

            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)

            self.scroll_to_bottom()

            channel_urls = self.collect_channel_detail_urls()

            self.process_channel_urls(channel_urls, category_name, output_file)

        except Exception as e:
            logger.error(f"Error scraping category: {e}")

    def save_results(self, filename):
        with open(filename, 'w', encoding="utf-8") as f:
            json.dump(self.channels_by_category, f, ensure_ascii=False, indent=4)

    def run(self, categories):
        try:
            base_url = "https://uk.tgstat.com"
            self.driver.get(base_url)
            cookies = pickle.load(open(config.COOKIES_FILE, "rb"))

            for cookie in cookies:
                self.driver.add_cookie(cookie)

            for category in categories:
                logger.info(f"Scraping category: {category}...")
                self.scrape_category(f"{base_url}/{category}", config.SCRAPPED_CHANNELS_FILE)

            return self.channels_by_category

        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
        finally:
            self.driver.quit()
