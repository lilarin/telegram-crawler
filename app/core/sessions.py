import glob
import os

from telethon import TelegramClient

from app.config import logger, config


class SessionManager:
    def __init__(self):
        self.sessions_dir = config.SESSIONS_DIR
        self.session_files = self._get_session_files()
        self.current_session_index = 0
        self.clients = {}

    def _get_session_files(self):
        session_pattern = os.path.join(self.sessions_dir, "*.session")
        return glob.glob(session_pattern)

    async def get_client(self):
        for _ in range(len(self.session_files)):
            session_file = self.session_files[self.current_session_index]
            session_name = os.path.splitext(os.path.basename(session_file))[0]

            if session_name not in self.clients:
                self.clients[session_name] = TelegramClient(
                    session=os.path.join(self.sessions_dir, session_name),
                    api_id=123,
                    api_hash="123",
                    device_model="MacBook Air M1",
                    system_version="macOS 14.4.1",
                    app_version="4.16.8 arm64",
                    lang_code="en",
                    system_lang_code="en",
                )
                await self.clients[session_name].connect()

                if not await self.clients[session_name].is_user_authorized():
                    self.rotate_session()
                    continue

            return self.clients[session_name], session_name

        return None, None

    def rotate_session(self):
        self.current_session_index = (self.current_session_index + 1) % len(
            self.session_files
        )

    async def close_all(self):
        for client in self.clients.values():
            await client.disconnect()
