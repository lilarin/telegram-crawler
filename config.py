import logging
import os
import sys


class Config:
    def __init__(self):
        self._load_env_vars()
        os.makedirs(self.SESSIONS_DIR, exist_ok=True)

    def _load_env_vars(self):
        # General settings
        self.SESSIONS_DIR = "../sessions"
        self.SCRAPPED_CHANNELS_FILE = "../scrapped_channels.json"
        self.SIMILAR_CHANNELS_FILE = "../processed_channels.json"
        self.COOKIES_FILE = "../cookies.pkl"
        
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
        return logging.getLogger("common-crawl")


config = Config()
logger = config.setup_logging()
