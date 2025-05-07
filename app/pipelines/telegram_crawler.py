import json
import os

from telethon import functions, types
from telethon.errors.rpcerrorlist import FloodWaitError
from telethon.tl.functions.channels import GetFullChannelRequest

from app.session_manager import SessionManager
from config import config, logger


class TelegramCrawler:
    def __init__(self):
        self.session_manager = SessionManager()
        self.result = {}
        self.processed_channels = set()
        self.processed_file = config.SIMILAR_CHANNELS_FILE

    @staticmethod
    async def get_channel_info(client, channel_url):
        try:
            entity = None
            full_chat = None

            if "joinchat" in channel_url:
                invite_hash = channel_url.split('/')[-1]
                try:
                    invite_result = await client(functions.messages.CheckChatInviteRequest(hash=invite_hash))

                    if hasattr(invite_result, 'chat'):
                        entity = invite_result.chat
                        # For joinchat links we don't have full_chat, so we create info directly
                        channel_info = {
                            'name': entity.title if hasattr(entity, 'title') else None,
                            'link': channel_url,
                            'id': entity.id if hasattr(entity, 'id') else None,
                            'subscribers': None,
                            'verified': entity.verified if hasattr(entity, 'verified') and entity.verified else False,
                            'created_at': entity.date.strftime('%d.%m.%Y') if hasattr(entity,
                                                                                      'date') and entity.date else None
                        }
                        return channel_info, entity
                    elif hasattr(invite_result, 'title'):
                        return {
                            'name': invite_result.title,
                            'link': channel_url,
                            'id': None,
                            'subscribers': invite_result.participants_count if hasattr(invite_result,
                                                                                       'participants_count') else None,
                            'verified': False,
                            'created_at': None
                        }, None
                    else:
                        logger.error(f"Unexpected invite result structure for {channel_url}")
                        return None, None

                except FloodWaitError:
                    raise
                except Exception as e:
                    logger.error(f"Error checking invite for {channel_url}: {e}")
                    return None, None
            else:
                username = channel_url.split('/')[-1]
                if username.startswith('@'):
                    username = username[1:]

                try:
                    input_entity = await client.get_input_entity(username)
                    result = await client(GetFullChannelRequest(input_entity))
                    entity = result.chats[0] if result.chats else None
                    full_chat = result.full_chat
                except FloodWaitError:
                    raise
                except Exception as e:
                    logger.error(f"Error getting input entity for {channel_url}: {e}")
                    return None, None

            if entity and full_chat:
                subscribers = getattr(full_chat, "participants_count", None)
                verified = entity.verified if hasattr(entity, 'verified') and entity.verified else False
                created_at = entity.date.strftime('%d.%m.%Y') if hasattr(entity, 'date') and entity.date else None

                channel_info = {
                    'name': entity.title,
                    'link': channel_url,
                    'id': entity.id,
                    'subscribers': subscribers,
                    'verified': verified,
                    'created_at': created_at
                }

                return channel_info, entity

        except FloodWaitError:
            raise
        except Exception as e:
            logger.error(f"Error getting channel info for {channel_url}: {e}")

        return None, None

    @staticmethod
    async def get_similar_channels(client, entity, channel_url):
        try:
            if hasattr(entity, "id") and hasattr(entity, "access_hash"):
                input_channel = types.InputChannel(
                    channel_id=entity.id,
                    access_hash=entity.access_hash
                )
            else:
                logger.warning(f"Channel entity missing required attributes: {channel_url}")
                return []

            result = await client(functions.channels.GetChannelRecommendationsRequest(channel=input_channel))

            if not result:
                return []

            similar_channels = []
            for ch in result.chats:
                if hasattr(ch, "username") and ch.username:
                    channel_link = f"https://t.me/{ch.username}"
                    subscribers = getattr(ch, "participants_count", getattr(ch, "members_count", None))
                    verified = ch.verified if hasattr(ch, 'verified') and ch.verified else False
                    created_at = ch.date.strftime('%d.%m.%Y') if hasattr(ch, 'date') and ch.date else None

                    similar_channels.append({
                        'name': ch.title,
                        'link': channel_link,
                        'id': ch.id,
                        'subscribers': subscribers,
                        'verified': verified,
                        'created_at': created_at
                    })

            return similar_channels

        except FloodWaitError:
            raise
        except Exception as e:
            logger.error(f"Error retrieving similar channels for {channel_url}: {e}")
            return []

    async def process_channel(self, channel_url, category):
        while True:
            client, session_name = await self.session_manager.get_client()
            if not client:
                logger.error("All sessions are unavailable. Cannot process channel.")
                return

            try:
                logger.info(f"Processing: {channel_url} with session: {session_name}")

                channel_info, entity = await self.get_channel_info(client, channel_url)
                if not entity:
                    logger.warning(f"Could not get channel info for: {channel_url}")
                    return

                messages = await client.get_messages(entity, limit=1)

                # similar_channels = await self.get_similar_channels(client, entity, channel_url)
                similar_channels = []

                if not similar_channels:
                    logger.warning(f"Cannot get similar channels for channel: {channel_url}")

                if category not in self.result:
                    self.result[category] = []

                channel_exists = False
                for existing_channel in self.result[category]:
                    if existing_channel.get("link") == channel_url:
                        channel_exists = True
                        break

                if not channel_exists:
                    channel_data = {
                        "name": channel_info.get("name"),
                        "link": channel_info.get("link"),
                        "id": channel_info.get("id"),
                        "subscribers": channel_info.get("subscribers"),
                        "verified": channel_info.get("verified"),
                        "created_at": channel_info.get("created_at"),
                        "related_channels": similar_channels
                    }

                    self.result[category].append(channel_data)

                self.processed_channels.add(channel_url)
                return

            except FloodWaitError:
                logger.error(f"Session {session_name} hit rate limit. Rotating to another session.")
                self.session_manager.rotate_session()

            except Exception as e:
                logger.error(f"Unexpected error processing channel {channel_url}: {e}")
                self.processed_channels.add(channel_url)
                return

    async def load_processed_data(self):
        if os.path.exists(self.processed_file):
            try:
                with open(self.processed_file, "r", encoding='utf-8') as f:
                    processed_data = json.load(f)
                    for category, channels in processed_data.items():
                        if category not in self.result:
                            self.result[category] = []
                        self.result[category].extend(channels)
                        for channel in channels:
                            self.processed_channels.add(channel["link"])
                logger.info(
                    f"Loaded {len(self.processed_channels)} already processed channels from {self.processed_file}")
            except Exception as e:
                logger.error(f"Error loading processed channels: {e}")

    async def save_progress(self):
        with open(self.processed_file, 'w', encoding='utf-8') as f:
            json.dump(self.result, f, ensure_ascii=False, indent=2)

    async def process_channels_by_category(self, channels_by_category):
        await self.load_processed_data()

        try:
            for category, channel_urls in channels_by_category.items():
                logger.info(f"Processing category: {category} with {len(channel_urls)} channels")

                for channel_url in channel_urls:
                    if channel_url in self.processed_channels:
                        logger.info(f"Skipping already processed channel: {channel_url}")
                        continue

                    await self.process_channel(channel_url, category)

                    await self.save_progress()

                logger.info(f"Completed processing category: {category}")
        finally:
            await self.session_manager.close_all()

        return self.result

    async def run(self):
        try:
            with open(config.SCRAPPED_CHANNELS_FILE, 'r', encoding='utf-8') as f:
                channels_by_category = json.load(f)
            logger.info(f"Loaded channel data from {config.SCRAPPED_CHANNELS_FILE}")

            await self.process_channels_by_category(channels_by_category)
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
