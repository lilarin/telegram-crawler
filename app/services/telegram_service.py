import base64
from typing import Dict, List, Optional, Tuple, Any

from telethon import functions, types
from telethon.errors.rpcerrorlist import FloodWaitError
from telethon.tl.functions.channels import GetFullChannelRequest

from app.config import logger
from app.core.database import async_session
from app.core.sessions import SessionManager
from app.repositories.category_repository import CategoryRepository
from app.repositories.channel_repository import ChannelRepository


class TelegramCrawler:
    def __init__(self):
        self.session_manager = SessionManager()
        self.processed_channels = set()
        self.processed_message_cache = {}
        self.batch_size = 100

    async def get_channel_from_url(self, client, channel_url: str) -> Tuple[Optional[Dict], Optional[Any]]:
        try:
            if "joinchat" in channel_url:
                return await self._process_invite_link(client, channel_url)
            else:
                return await self._process_public_channel(client, channel_url)
        except FloodWaitError:
            raise
        except Exception as e:
            logger.error(f"Error getting channel info for {channel_url}: {e}")
            return None, None

    async def _process_invite_link(self, client, channel_url: str) -> Tuple[Optional[Dict], Optional[Any]]:
        invite_hash = channel_url.split("/")[-1]
        try:
            invite_result = await client(functions.messages.CheckChatInviteRequest(hash=invite_hash))

            if hasattr(invite_result, "chat"):
                entity = invite_result.chat
                channel_info = self._extract_basic_channel_info(entity, channel_url)
                return channel_info, entity
            elif hasattr(invite_result, "title"):
                return {
                    "name": invite_result.title,
                    "link": channel_url,
                    "id": None,
                    "subscribers": getattr(invite_result, "participants_count", None),
                    "verified": False,
                    "created_at": None,
                }, None
            else:
                logger.error(f"Unexpected invite result structure for {channel_url}")
                return None, None
        except Exception as e:
            logger.error(f"Error checking invite for {channel_url}: {e}")
            return None, None

    async def _process_public_channel(self, client, channel_url: str) -> Tuple[Optional[Dict], Optional[Any]]:
        username = channel_url.split("/")[-1]
        if username.startswith("@"):
            username = username[1:]

        try:
            input_entity = await client.get_input_entity(username)
            result = await client(GetFullChannelRequest(input_entity))
            entity = result.chats[0] if result.chats else None
            full_chat = result.full_chat

            if entity and full_chat:
                channel_info = self._extract_channel_info(entity, full_chat, channel_url)
                return channel_info, entity
            return None, None
        except Exception as e:
            logger.error(f"Error getting input entity for {channel_url}: {e}")
            return None, None

    async def get_channel_info_by_id(self, client, channel_id: int) -> Tuple[Optional[Dict], Optional[Any]]:
        try:
            entity = await client.get_entity(channel_id)
            if entity:
                channel_link = f"https://t.me/{entity.username}" if hasattr(entity,
                                                                            "username") and entity.username else None
                channel_info = self._extract_basic_channel_info(entity, channel_link)
                channel_info["id"] = channel_id
                return channel_info, entity
        except Exception as e:
            logger.error(f"Error getting channel info for ID {channel_id}: {e}")
        return None, None

    @staticmethod
    def _extract_basic_channel_info(entity, channel_url: Optional[str]) -> Dict[str, Any]:
        return {
            "name": entity.title if hasattr(entity, "title") else f"Channel {getattr(entity, 'id', 'Unknown')}",
            "link": channel_url,
            "id": getattr(entity, "id", None),
            "subscribers": getattr(entity, "participants_count", None),
            "verified": getattr(entity, "verified", False),
            "created_at": entity.date.strftime("%d.%m.%Y") if hasattr(entity, "date") and entity.date else None,
        }

    def _extract_channel_info(self, entity, full_chat, channel_url: str) -> Dict[str, Any]:
        channel_info = self._extract_basic_channel_info(entity, channel_url)
        channel_info["subscribers"] = getattr(full_chat, "participants_count", None)
        return channel_info

    async def get_similar_channels(self, client, entity, channel_url: str) -> List[Dict]:
        try:
            if not (hasattr(entity, "id") and hasattr(entity, "access_hash")):
                logger.warning(f"Channel entity missing required attributes: {channel_url}")
                return []

            input_channel = types.InputChannel(channel_id=entity.id, access_hash=entity.access_hash)
            result = await client(functions.channels.GetChannelRecommendationsRequest(channel=input_channel))

            if not result:
                return []

            similar_channels = []
            for ch in result.chats:
                if hasattr(ch, "username") and ch.username:
                    channel_link = f"https://t.me/{ch.username}"
                    similar_channels.append(self._extract_basic_channel_info(ch, channel_link))

            return similar_channels
        except FloodWaitError:
            raise
        except Exception as e:
            logger.error(f"Error retrieving similar channels for {channel_url}: {e}")
            return []

    async def get_channel_messages(
            self, client, entity, offset_id: int, min_id: Optional[int] = None) -> List:
        try:
            messages = []

            if min_id:
                async for message in client.iter_messages(entity, min_id=min_id, limit=self.batch_size,
                                                          offset_id=offset_id, reverse=True):
                    messages.append(message)
            else:
                async for message in client.iter_messages(entity, limit=self.batch_size, offset_id=offset_id,
                                                          reverse=True):
                    messages.append(message)

            return messages
        except FloodWaitError:
            print("flood")
        except Exception as e:
            logger.error(f"Error getting channel messages: {e}")
            return []

    @staticmethod
    async def extract_forwarded_channels(messages, main_channel_id=None) -> List[int]:
        """Extract channel IDs from forwarded messages"""
        try:
            forwarded_channels = []
            for message in messages:
                if message.fwd_from and hasattr(message.fwd_from, "from_id"):
                    try:
                        if hasattr(message.fwd_from.from_id, "channel_id"):
                            channel_id = message.fwd_from.from_id.channel_id
                            if (channel_id and channel_id != main_channel_id
                                    and channel_id not in forwarded_channels):
                                forwarded_channels.append(channel_id)
                    except Exception as e:
                        logger.error(f"Error extracting forwarded channel ID: {e}")
                        continue

            logger.info(f"Found {len(forwarded_channels)} related channel IDs")
            return forwarded_channels
        except Exception as e:
            logger.error(f"Error getting related channel IDs: {e}")
            return []

    def _sanitize_for_json(self, data: Any) -> Any:
        """Recursively convert non-serializable objects to serializable format"""
        if isinstance(data, bytes):
            return base64.b64encode(data).decode("utf-8")
        elif isinstance(data, dict):
            return {k: self._sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._sanitize_for_json(item) for item in data]
        elif hasattr(data, "isoformat") and callable(getattr(data, "isoformat")):
            return data.isoformat()
        else:
            return data

    async def extract_message_data(self, client, entity, message) -> Dict:
        fwd_from = None
        if message.fwd_from and hasattr(message.fwd_from, "from_id"):
            if hasattr(message.fwd_from.from_id, "channel_id"):
                fwd_from = message.fwd_from.from_id.channel_id

        reactions_list = []
        if message.reactions:
            for reaction in message.reactions.results:
                if hasattr(reaction.reaction, "emoticon"):
                    reactions_list.append({
                        "count": reaction.count,
                        "emoji": reaction.reaction.emoticon,
                    })

        urls = []
        if message.entities:
            for entity_item in message.entities:
                if hasattr(entity_item, "url") and entity_item.url:
                    urls.append(entity_item.url)

        media_list = []
        try:
            if message.grouped_id:
                media_messages = []
                # Use iter_messages instead of get_messages for grouped media
                async for msg in client.iter_messages(
                        entity,
                        min_id=message.id - 10,
                        max_id=message.id + 10
                ):
                    if msg.grouped_id == message.grouped_id:
                        media_messages.append(msg)
            else:
                media_messages = [message]

            for msg in media_messages:
                if msg.media:
                    try:
                        media_dict = msg.media.to_dict()
                        media_list.append(media_dict)
                    except Exception as e:
                        logger.warning(f"Could not convert media to dict: {e}")
                        media_list.append({"type": str(type(msg.media))})
        except Exception as e:
            logger.warning(f"Error processing media: {e}")
            media_list = None

        message_dict = {
            'id': message.id,
            'date': message.date,
            'message': message.message,
            'reactions': reactions_list,
            'fwd_from': fwd_from,
            'urls': urls,
            'media': media_list
        }

        return self._sanitize_for_json(message_dict)

    async def save_channel_messages(self, client, channel, entity, messages):
        try:
            async with async_session() as db_session:
                channel_repo = ChannelRepository(db_session)
                processed_ids = set()

                for message in messages:
                    if message.id in processed_ids:
                        continue

                    message_data = await self.extract_message_data(client, entity, message)
                    await channel_repo.save_channel_message(channel.id, message.id, message_data)
                    processed_ids.add(message.id)

                logger.info(f"Saved {len(processed_ids)} messages for channel {channel.name}")
        except Exception as e:
            logger.error(f"Error saving channel messages: {e}")

    @staticmethod
    async def get_latest_stored_message_id(channel_id: int) -> Optional[int]:
        try:
            async with async_session() as db_session:
                channel_repo = ChannelRepository(db_session)
                latest_message_id = await channel_repo.get_latest_message_id(channel_id)
                return latest_message_id
        except Exception as e:
            logger.error(f"Error getting latest message ID for channel {channel_id}: {e}")
            return None

    async def process_channel(self, channel_url: str):
        """Process a single channel - get info, similar channels, and related channels"""
        async with async_session() as db_session:
            channel_repo = ChannelRepository(db_session)

            while True:
                client, session_name = await self.session_manager.get_client()
                if not client:
                    logger.error("All sessions are unavailable. Cannot process channel.")
                    return

                try:
                    logger.info(f"Processing: {channel_url} with session: {session_name}")

                    # Get channel info
                    channel_info, entity = await self.get_channel_from_url(client, channel_url)
                    if not channel_info or not entity:
                        logger.warning(f"Could not get channel info for: {channel_url}")
                        return

                    # Save main channel
                    main_channel = await channel_repo.get_or_create_channel(channel_info)
                    if not main_channel:
                        logger.error(f"Failed to create/update main channel: {channel_url}")
                        return

                    # Get and save similar channels
                    await self._process_similar_channels(client, channel_repo, entity, channel_url, main_channel)

                    # Get and process messages with batching
                    await self._process_channel_messages(client, main_channel, entity)

                    # Process related channels from forwarded messages (using the all_messages we collected)
                    await self._process_related_channels(
                        client,
                        channel_repo,
                        self.processed_message_cache.get(main_channel.id, []),
                        main_channel
                    )

                    self.processed_channels.add(channel_url)
                    return

                except FloodWaitError:
                    logger.error(f"Session {session_name} hit rate limit. Rotating to another session.")
                    self.session_manager.rotate_session()
                except Exception as e:
                    logger.error(f"Unexpected error processing channel {channel_url}: {e}")
                    self.processed_channels.add(channel_url)
                    return

    async def _process_channel_messages(self, client, channel, entity):
        """Get and process channel messages in batches, starting from the oldest not yet processed"""
        # Get the latest channel message ID we have in the database
        latest_id = await self.get_latest_stored_message_id(channel.id)

        offset_id = 0
        all_messages = []
        processed_message_ids = set()

        # Get messages in batches
        while True:
            batch = await self.get_channel_messages(
                client,
                entity,
                min_id=latest_id,
                offset_id=offset_id
            )

            if not batch:
                logger.info(f"No more messages to fetch for channel {channel.name}, stopped on ID: {offset_id}")
                break

            new_messages = [m for m in batch if m.id not in processed_message_ids]

            if not new_messages:
                logger.info(f"No new messages in this batch for channel {channel.name}")
                break

            # Update tracking variables
            processed_message_ids.update(m.id for m in new_messages)
            all_messages.extend(new_messages)

            # Save this batch of messages
            await self.save_channel_messages(client, channel, entity, new_messages)

            # Get the maximum ID in this batch to use as the next offset
            max_id_in_batch = max(m.id for m in batch)
            if max_id_in_batch == offset_id or len(batch) < self.batch_size:
                break
            offset_id = max_id_in_batch

            logger.info(
                f"Fetched and processed {len(new_messages)} messages, total so far: {len(all_messages)}, current ID: {offset_id}")

        logger.info(f"Total new messages processed for {channel.name}: {len(all_messages)}")
        # Store all messages for later related channel processing
        self.processed_message_cache[channel.id] = all_messages

    async def _process_similar_channels(self, client, channel_repo, entity, channel_url, main_channel):
        similar_channels_data = await self.get_similar_channels(client, entity, channel_url)
        logger.info(f"Found {len(similar_channels_data)} similar channels")

        for similar_data in similar_channels_data:
            similar_channel = await channel_repo.get_or_create_channel(similar_data)
            if similar_channel:
                await channel_repo.add_similar_channel(main_channel, similar_channel)

    async def _process_related_channels(self, client, channel_repo, messages, main_channel):
        """Process and save related channels from forwarded messages"""
        main_channel_id = getattr(main_channel, 'channel_id', None)
        related_channel_ids = await self.extract_forwarded_channels(messages, main_channel_id)

        for related_id in related_channel_ids:
            related_info, related_entity = await self.get_channel_info_by_id(client, related_id)
            if related_info:
                related_channel = await channel_repo.get_or_create_channel(related_info)
                if related_channel:
                    await channel_repo.add_related_channel(main_channel, related_channel)
            else:
                logger.warning(f"Could not get info for related channel ID: {related_id}")

        logger.info(f"Added {len(related_channel_ids)} related channels")

    async def process_channels_by_category(self, categories: List[str]):
        try:
            async with async_session() as db_session:
                category_repo = CategoryRepository(db_session)

                for category in categories:
                    logger.info(f"Processing category: {category}")
                    channel_urls = await category_repo.get_channel_urls_by_category(category)
                    logger.info(f"Found {len(channel_urls)} channels for category {category}")

                    for channel_url in channel_urls:
                        if channel_url in self.processed_channels:
                            logger.info(f"Skipping already processed channel: {channel_url}")
                            continue
                        await self.process_channel(channel_url)

                    logger.info(f"Completed processing category: {category}")
        finally:
            await self.session_manager.close_all()

    async def run(self, categories: Optional[List[str]] = None):
        try:
            if categories is None:
                async with async_session() as db_session:
                    category_repo = CategoryRepository(db_session)
                    categories = await category_repo.get_all_category_names()

            logger.info(f"Processing categories: {categories}")
            await self.process_channels_by_category(categories)
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
