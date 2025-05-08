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

    @staticmethod
    async def get_channel_info(client, channel_url):
        try:
            entity = None
            full_chat = None

            if "joinchat" in channel_url:
                invite_hash = channel_url.split('/')[-1]
                try:
                    invite_result = await client(
                        functions.messages.CheckChatInviteRequest(hash=invite_hash)
                    )

                    if hasattr(invite_result, 'chat'):
                        entity = invite_result.chat
                        # For joinchat links we don't have full_chat, so we create info directly
                        channel_info = {
                            'name': entity.title if hasattr(entity, 'title') else None,
                            'link': channel_url,
                            'id': entity.id if hasattr(entity, 'id') else None,
                            'subscribers': None,
                            'verified': entity.verified if hasattr(entity, 'verified') and entity.verified else False,
                            'created_at': entity.date.strftime('%d.%m.%Y') if hasattr(entity, 'date') and entity.date else None
                        }
                        return channel_info, entity
                    elif hasattr(invite_result, 'title'):
                        return {
                            'name': invite_result.title,
                            'link': channel_url,
                            'id': None,
                            'subscribers': invite_result.participants_count if hasattr(invite_result, 'participants_count') else None,
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
                verified = entity.verified if hasattr(entity, "verified") and entity.verified else False
                created_at = entity.date.strftime("%d.%m.%Y") if hasattr(entity, "date") and entity.date else None

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
    async def format_similar_channels(client, entity, channel_url):
        try:
            if hasattr(entity, "id") and hasattr(entity, "access_hash"):
                input_channel = types.InputChannel(
                    channel_id=entity.id,
                    access_hash=entity.access_hash
                )
            else:
                logger.warning(f"Channel entity missing required attributes: {channel_url}")
                return []

            result = await client(
                functions.channels.GetChannelRecommendationsRequest(channel=input_channel)
            )

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
        async with async_session() as db_session:
            channel_repo = ChannelRepository(db_session)

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

                    # Save main channel to database
                    main_channel = await channel_repo.get_or_create_channel(channel_info)
                    if not main_channel:
                        logger.error(f"Failed to create/update main channel: {channel_url}")
                        return

                    # Get similar channels
                    similar_channels_data = await self.format_similar_channels(client, entity, channel_url)

                    # Save similar channels to database and create relationships
                    for similar_data in similar_channels_data:
                        similar_channel = await channel_repo.get_or_create_channel(similar_data)
                        if similar_channel:
                            await channel_repo.add_similar_channel(main_channel, similar_channel)

                    self.processed_channels.add(channel_url)
                    return

                except FloodWaitError:
                    logger.error(f"Session {session_name} hit rate limit. Rotating to another session.")
                    self.session_manager.rotate_session()

                except Exception as e:
                    logger.error(f"Unexpected error processing channel {channel_url}: {e}")
                    self.processed_channels.add(channel_url)
                    return

    async def process_channels_by_category(self, categories):
        try:
            # Load channels for each category from database
            async with async_session() as db_session:
                category_repo = CategoryRepository(db_session)

                for category in categories:
                    logger.info(f"Processing category: {category}")

                    # Get channel links for this category
                    channel_urls = await category_repo.get_channel_urls_by_category(category)
                    logger.info(f"Found {len(channel_urls)} channels for category {category}")

                    for channel_url in channel_urls:
                        if channel_url in self.processed_channels:
                            logger.info(f"Skipping already processed channel: {channel_url}")
                            continue

                        await self.process_channel(channel_url, category)

                    logger.info(f"Completed processing category: {category}")
        finally:
            await self.session_manager.close_all()

    async def run(self, categories=None):
        try:
            if categories is None:
                # If no categories specified, get all categories from database
                async with async_session() as db_session:
                    category_repo = CategoryRepository(db_session)
                    categories = await category_repo.get_all_category_names()

            logger.info(f"Processing categories: {categories}")
            await self.process_channels_by_category(categories)

        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
