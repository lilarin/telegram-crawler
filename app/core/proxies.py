import json
import random
from typing import Dict, Optional, List

import requests

from app.config import logger, config


class ProxiesManager:
    def __init__(self):
        self.proxy_url = config.PROXY_URL
        self.proxies: List[Dict] = []
        self._fetch_proxies()

    def _fetch_proxies(self):
        if not self.proxy_url:
            logger.warning("No proxy URL configured, skipping proxy fetch")
            return

        telethon_proxies = []

        try:
            with requests.Session() as session:
                response = session.get(self.proxy_url, timeout=10)
                response.raise_for_status()
                data = response.json()

                if not isinstance(data, dict) or "proxies" not in data or not isinstance(data["proxies"], list):
                    logger.error("Invalid proxy data format")
                    return

                for proxy_info in data["proxies"]:
                    if not isinstance(proxy_info, dict):
                        continue

                    if round(proxy_info.get("timeout", float('inf'))) > 500:
                        continue

                    protocol = proxy_info.get("protocol")
                    ip = proxy_info.get("ip")
                    port = proxy_info.get("port")

                    if protocol == "http" and ip and port:
                        try:
                            port = int(port)
                            telethon_proxy = {
                                "proxy_type": "http",
                                "addr": ip,
                                "port": port,
                                "rdns": True
                            }
                            telethon_proxies.append(telethon_proxy)
                        except ValueError:
                            logger.warning(f"Invalid port format for {ip}:{port}")
                            continue

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request error: {e}")
        except json.JSONDecodeError:
            logger.error("JSON decoding error.")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        self.proxies = telethon_proxies
        logger.info(f"Fetched {len(self.proxies)} proxies")

    def get_random_proxy(self) -> Optional[Dict]:
        if not self.proxies:
            self._fetch_proxies()
        if not self.proxies:
            return None
        return random.choice(self.proxies)
