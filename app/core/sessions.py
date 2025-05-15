import asyncio
import glob
import os
import random
from typing import Dict, Optional, Tuple, Set

from telethon import TelegramClient

from app.config import logger, config
from app.core.proxies import ProxiesManager


class SessionManager:
    def __init__(self):
        self.sessions_dir = config.SESSIONS_DIR
        self.session_files = self._get_session_files()
        self.current_session_index = 0
        self.clients: Dict[str, TelegramClient] = {}
        self.busy_sessions: Set[str] = set()
        self.banned_sessions: Set[str] = set()
        self.lock = asyncio.Lock()
        self.proxy_manager = ProxiesManager()

    def _get_session_files(self):
        session_pattern = os.path.join(self.sessions_dir, "*.session")
        return glob.glob(session_pattern)

    def mark_session_banned(self, session_name: str):
        if session_name in self.clients:
            logger.warning(f"Session {session_name} has been banned and will no longer be used")
            self.banned_sessions.add(session_name)
            if session_name in self.busy_sessions:
                self.busy_sessions.remove(session_name)

    def get_available_session_count(self):
        return len(self.session_files) - len(self.banned_sessions)

    async def get_client(self) -> Tuple[Optional[TelegramClient], Optional[str]]:
        async with self.lock:
            if len(self.banned_sessions) >= len(self.session_files):
                logger.error("All sessions are banned. Cannot proceed.")
                return None, None

            # Try to find an available session that's not busy and not banned
            for _ in range(len(self.session_files)):
                session_file = self.session_files[self.current_session_index]
                session_name = os.path.splitext(os.path.basename(session_file))[0]

                self.rotate_session()

                if session_name in self.busy_sessions or session_name in self.banned_sessions:
                    continue

                if session_name not in self.clients:
                    # Random delay to avoid too many connections at once
                    delay = random.uniform(0.5, 3.0)
                    await asyncio.sleep(delay)

                    # Get a random proxy
                    proxy = self.proxy_manager.get_random_proxy()
                    proxy_str = f"{proxy['addr']}:{proxy['port']}"

                    client_args = {
                        "session": os.path.join(self.sessions_dir, session_name),
                        "api_id": 123,
                        "api_hash": "123",
                        "device_model": "MacBook Air M1",
                        "system_version": "macOS 14.4.1",
                        "app_version": "4.16.8 arm64",
                        "lang_code": "en",
                        "system_lang_code": "en",
                    }

                    if proxy:
                        client_args["proxy"] = proxy
                        logger.info(f"Using proxy {proxy_str} for session {session_name}")

                    self.clients[session_name] = TelegramClient(**client_args)

                    try:
                        await self.clients[session_name].connect()
                    except Exception as e:
                        logger.error(f"Failed to connect session {session_name} with proxy {proxy_str}: {e}")
                        continue

                    if not await self.clients[session_name].is_user_authorized():
                        logger.warning(f"Session {session_name} is not authorized")
                        continue

                # Mark this session as busy
                self.busy_sessions.add(session_name)
                logger.info(f"Allocated session: {session_name}")
                return self.clients[session_name], session_name

            # No available sessions
            return None, None

    def release_client(self, session_name: str):
        if session_name in self.busy_sessions:
            self.busy_sessions.remove(session_name)
            logger.info(f"Released session: {session_name}")

    def rotate_session(self):
        self.current_session_index = (self.current_session_index + 1) % len(
            self.session_files
        )

    async def close_all(self):
        for client in self.clients.values():
            await client.disconnect()
