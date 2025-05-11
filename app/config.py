import logging
import os
import sys


class Config:
    def __init__(self):
        self._load_env_vars()


    def _load_env_vars(self):
        # General settings
        self.SESSIONS_DIR = "../sessions"
        self.COOKIES_FILE = "../cookies.pkl"
        self.BASE_URL = "https://uk.tgstat.com"

        # Database settings
        self.POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
        self.POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
        self.POSTGRES_DB = os.environ.get("POSTGRES_DB", "telegram_crawler")
        self.POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
        self.POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
        self.DATABASE_URL = f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

        # Neo4j settings
        self.NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
        self.NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
        self.NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

    @staticmethod
    def setup_logging():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(sys.stdout),
            ],
        )
        logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)

        return logging.getLogger("common-crawl")


config = Config()
logger = config.setup_logging()
