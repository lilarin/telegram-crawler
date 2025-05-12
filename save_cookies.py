import pickle
import time

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from app.config import config


def save_cookies():
    ua = UserAgent()
    user_agent = ua.random

    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("window-size=1280,720")
    options.add_argument(f"user-agent={user_agent}")

    driver = webdriver.Chrome(options=options)

    driver.get(config.BASE_URL)

    time.sleep(15)

    cookies = driver.get_cookies()

    with open(config.COOKIES_FILE, 'wb') as file:
        pickle.dump(cookies, file)


if __name__ == "__main__":
    save_cookies()
