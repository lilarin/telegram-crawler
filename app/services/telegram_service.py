import base64
import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set

from telethon import functions, types
from telethon.errors.rpcerrorlist import FloodWaitError, UserDeactivatedBanError
from telethon.tl.functions.channels import GetFullChannelRequest

from app.config import logger
from app.core.database import async_session
from app.core.sessions import SessionManager
from app.repositories.category_repository import CategoryRepository
from app.repositories.channel_repository import ChannelRepository


class TelegramCrawler:
    def __init__(self, max_workers: int):
        self.session_manager = SessionManager()
        self.processed_channels: Set[str] = set()
        self.processed_message_cache = {}
        self.batch_size = 100
        self.max_workers = max_workers
        self.channel_queue = asyncio.Queue()
        self.active_workers = 0
        self.worker_lock = asyncio.Lock()

    async def get_channel_from_url(
            self, client, channel_url: str, session_name: str
    ) -> Tuple[Optional[Dict], Optional[Any]]:
        try:
            if "joinchat" in channel_url:
                return await self._process_invite_link(client, channel_url, session_name)
            else:
                return await self._process_public_channel(client, channel_url, session_name)
        except (FloodWaitError, UserDeactivatedBanError):
            raise
        except Exception as e:
            logger.error(f"[{session_name}] Error getting channel info for {channel_url}: {e}")
            return None, None

    async def _process_invite_link(
            self, client, channel_url: str, session_name: str
    ) -> Tuple[Optional[Dict], Optional[Any]]:
        invite_hash = channel_url.split("/")[-1]
        try:
            invite_result = await client(
                functions.messages.CheckChatInviteRequest(hash=invite_hash)
            )

            if hasattr(invite_result, "chat"):
                entity = invite_result.chat
                channel_info = self._extract_basic_channel_info(entity, channel_url)
                return channel_info, entity
            else:
                logger.error(f"[{session_name}] Unexpected invite result structure for {channel_url}")
                return None, None
        except (FloodWaitError, UserDeactivatedBanError):
            raise
        except Exception as e:
            logger.error(f"[{session_name}] Error checking invite for {channel_url}: {e}")
            return None, None

    async def _process_public_channel(
            self, client, channel_url: str, session_name: str
    ) -> Tuple[Optional[Dict], Optional[Any]]:
        username = channel_url.split("/")[-1]
        if username.startswith("@"):
            username = username[1:]

        input_entity = None

        try:
            input_entity = await client.get_input_entity(username)
            result = await client(GetFullChannelRequest(input_entity))
            entity = result.chats[0] if result.chats else None
            full_chat = result.full_chat

            if entity and full_chat:
                channel_info = self._extract_channel_info(
                    entity, full_chat, channel_url
                )
                return channel_info, entity
            return None, None
        except ValueError:
            try:
                if input_entity:
                    channel_info = self._extract_basic_channel_info(
                        input_entity, channel_url
                    )
                    return channel_info, input_entity
                return None, None
            except Exception:
                raise
        except (FloodWaitError, UserDeactivatedBanError):
            raise
        except Exception as e:
            logger.error(f"[{session_name}] Error getting input entity for {channel_url}: {e} ({type(e)})")
            return None, None

    async def get_channel_info_by_id(
            self, client, channel_id: int, session_name: str
    ) -> Tuple[Optional[Dict], Optional[Any]]:
        try:
            entity = await client.get_entity(channel_id)
            result = await client(GetFullChannelRequest(entity))
            entity = result.chats[0] if result.chats else None
            full_chat = result.full_chat
            if entity:
                channel_link = (
                    f"https://t.me/{entity.username}"
                    if hasattr(entity, "username") and entity.username
                    else None
                )
                channel_info = self._extract_channel_info(entity, full_chat, channel_link)
                channel_info["id"] = channel_id
                return channel_info, entity
            return None, None
        except (FloodWaitError, UserDeactivatedBanError):
            raise
        except Exception as e:
            logger.error(f"[{session_name}] Error getting channel info for ID {channel_id}: {e}")
            return None, None

    @staticmethod
    def _extract_basic_channel_info(
            entity, channel_url: Optional[str]
    ) -> Dict[str, Any]:
        return {
            "name": (
                entity.title
                if hasattr(entity, "title")
                else f"Channel {getattr(entity, 'id', 'Unknown')}"
            ),
            "link": channel_url,
            "id": getattr(entity, "id", None),
            "subscribers": getattr(entity, "participants_count", None),
            "verified": getattr(entity, "verified", False),
            "created_at": (
                entity.date.strftime("%d.%m.%Y")
                if hasattr(entity, "date") and entity.date
                else None
            ),
        }

    def _extract_channel_info(
            self, entity, full_chat, channel_url: str
    ) -> Dict[str, Any]:
        channel_info = self._extract_basic_channel_info(entity, channel_url)
        channel_info["subscribers"] = getattr(full_chat, "participants_count", None)
        return channel_info

    async def get_similar_channels(
            self, client, entity, channel_url: str, session_name: str
    ) -> List[Dict]:
        try:
            if not (hasattr(entity, "id") and hasattr(entity, "access_hash")):
                logger.warning(
                    f"[{session_name}] Channel entity missing required attributes: {channel_url}"
                )
                return []

            input_channel = types.InputChannel(
                channel_id=entity.id, access_hash=entity.access_hash
            )
            result = await client(
                functions.channels.GetChannelRecommendationsRequest(
                    channel=input_channel
                )
            )

            if not result:
                return []

            similar_channels = []
            for ch in result.chats:
                if hasattr(ch, "username") and ch.username:
                    channel_link = f"https://t.me/{ch.username}"
                    try:
                        result = await client(GetFullChannelRequest(ch))
                        full_chat = result.full_chat
                        similar_channels.append(
                            self._extract_channel_info(ch, full_chat, channel_link)
                        )
                    except Exception as e:
                        logger.warning(f"[{session_name}] Error getting details for similar channel {channel_link}: {e}")
                        continue

            return similar_channels
        except (FloodWaitError, UserDeactivatedBanError):
            # Let these errors propagate up for special handling
            raise
        except Exception as e:
            logger.error(f"[{session_name}] Error retrieving similar channels for {channel_url}: {e}")
            return []

    async def get_channel_messages(
            self, client, entity, offset_id: int, min_id: Optional[int] = None, session_name: str = ""
    ) -> List:
        try:
            messages = []

            if min_id:
                async for message in client.iter_messages(
                        entity,
                        min_id=min_id,
                        limit=self.batch_size,
                        offset_id=offset_id,
                        reverse=True,
                ):
                    messages.append(message)
            else:
                async for message in client.iter_messages(
                        entity, limit=self.batch_size, offset_id=offset_id, reverse=True
                ):
                    messages.append(message)

            return messages
        except FloodWaitError:
            logger.warning(f"[{session_name}] Flood wait error when fetching messages")
            raise
        except Exception as e:
            logger.error(f"[{session_name}] Error getting channel messages: {e}")
            return []

    @staticmethod
    async def extract_forwarded_channels(messages, main_channel_id=None, session_name: str = "") -> List[int]:
        """Extract channel IDs from forwarded messages"""
        try:
            forwarded_channels = []
            for message in messages:
                if message.fwd_from and hasattr(message.fwd_from, "from_id"):
                    try:
                        if hasattr(message.fwd_from.from_id, "channel_id"):
                            channel_id = message.fwd_from.from_id.channel_id
                            if (
                                    channel_id
                                    and channel_id != main_channel_id
                                    and channel_id not in forwarded_channels
                            ):
                                forwarded_channels.append(channel_id)
                    except Exception as e:
                        logger.error(f"[{session_name}] Error extracting forwarded channel ID: {e}")
                        continue

            logger.info(f"[{session_name}] Found {len(forwarded_channels)} related channel IDs")
            return forwarded_channels
        except Exception as e:
            logger.error(f"[{session_name}] Error getting related channel IDs: {e}")
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

    async def extract_message_data(self, client, entity, message, session_name: str) -> Dict:
        fwd_from = None
        if message.fwd_from and hasattr(message.fwd_from, "from_id"):
            if hasattr(message.fwd_from.from_id, "channel_id"):
                fwd_from = message.fwd_from.from_id.channel_id

        reactions_list = []
        if message.reactions:
            for reaction in message.reactions.results:
                if hasattr(reaction.reaction, "emoticon"):
                    reactions_list.append(
                        {
                            "count": reaction.count,
                            "emoji": reaction.reaction.emoticon,
                        }
                    )

        urls = []
        if message.entities:
            for entity_item in message.entities:
                if hasattr(entity_item, "url") and entity_item.url:
                    urls.append(entity_item.url)

        media_list = []
        try:
            if message.grouped_id:
                media_messages = []
                async for msg in client.iter_messages(
                        entity, min_id=message.id - 10, max_id=message.id + 10
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
                        logger.warning(f"[{session_name}] Could not convert media to dict: {e}")
                        media_list.append({"type": str(type(msg.media))})
        except Exception as e:
            logger.warning(f"[{session_name}] Error processing media: {e}")
            media_list = None

        message_dict = {
            "id": message.id,
            "date": message.date,
            "message": message.message,
            "reactions": reactions_list,
            "fwd_from": fwd_from,
            "urls": urls,
            "media": media_list,
        }

        return self._sanitize_for_json(message_dict)

    async def save_channel_messages(self, client, channel, entity, messages, session_name: str):
        try:
            async with async_session() as db_session:
                channel_repo = ChannelRepository(db_session)
                processed_ids = set()

                for message in messages:
                    if message.id in processed_ids:
                        continue

                    message_data = await self.extract_message_data(
                        client, entity, message, session_name
                    )
                    await channel_repo.save_channel_message(
                        channel.id, message.id, message_data
                    )
                    processed_ids.add(message.id)

                logger.info(
                    f"[{session_name}] Saved {len(processed_ids)} messages for channel {channel.name}"
                )
        except Exception as e:
            logger.error(f"[{session_name}] Error saving channel messages: {e}")

    @staticmethod
    async def get_latest_stored_message_id(channel_id: int) -> Optional[int]:
        try:
            async with async_session() as db_session:
                channel_repo = ChannelRepository(db_session)
                latest_message_id = await channel_repo.get_latest_message_id(channel_id)
                return latest_message_id
        except Exception as e:
            logger.error(
                f"Error getting latest message ID for channel {channel_id}: {e}"
            )
            return None

    async def process_channel(self, channel_url: str):
        """Process a single channel - get info, similar channels, and related channels"""
        session_name = None
        
        try:
            client, session_name = await self.session_manager.get_client()
            if not client:
                logger.error("All sessions are unavailable. Cannot process channel.")
                return False

            logger.info(f"[{session_name}] Processing: {channel_url}")
            
            async with async_session() as db_session:
                channel_repo = ChannelRepository(db_session)

                try:
                    # Get channel info
                    channel_info, entity = await self.get_channel_from_url(
                        client, channel_url, session_name
                    )
                    if not channel_info or not entity:
                        logger.warning(f"[{session_name}] Could not get channel info for: {channel_url}")
                        return True  # Consider this done (no retry)

                    # Save main channel
                    main_channel = await channel_repo.get_or_create_channel(channel_info)
                    if not main_channel:
                        logger.error(
                            f"[{session_name}] Failed to create/update "
                            f"main channel: {channel_url}"
                        )
                        return True  # Consider this done (no retry)

                    # Get and save similar channels
                    await self._process_similar_channels(
                        client,
                        channel_repo,
                        entity,
                        channel_url,
                        main_channel,
                        session_name
                    )

                    # Get and process messages with batching
                    await self._process_channel_messages(
                        client,
                        main_channel,
                        entity,
                        session_name
                    )

                    # Process related channels from forwarded messages
                    await self._process_related_channels(
                        client,
                        channel_repo,
                        self.processed_message_cache.get(main_channel.id, []),
                        main_channel,
                        session_name
                    )

                    self.processed_channels.add(channel_url)
                    logger.info(f"[{session_name}] Completed processing channel: {channel_url}")
                    return True
                    
                except (FloodWaitError, UserDeactivatedBanError):
                    # Let these propagate to the outer handler
                    raise

        except FloodWaitError as e:
            wait_time = getattr(e, 'seconds', 60)
            logger.error(f"[{session_name}] Session hit rate limit, must wait {wait_time} seconds")
            return False  # Retry with another session
            
        except UserDeactivatedBanError:
            logger.error(f"[{session_name}] Session banned permanently")
            # Mark this session as banned so it won't be used again
            if session_name:
                self.session_manager.mark_session_banned(session_name)
            return False  # Retry with another session
            
        except Exception as e:
            logger.error(f"[{session_name}] Unexpected error processing channel {channel_url}: {e}")
            self.processed_channels.add(channel_url)  # Don't retry on general errors
            return True
            
        finally:
            # Always release the client when done
            if session_name:
                self.session_manager.release_client(session_name)

    async def _process_channel_messages(self, client, channel, entity, session_name: str):
        """Get and process channel messages in batches, starting from the oldest not yet processed"""
        # Get the latest channel message ID we have in the database
        latest_id = await self.get_latest_stored_message_id(channel.id)

        offset_id = 0
        all_messages = []
        processed_message_ids = set()

        # Get messages in batches
        while True:
            batch = await self.get_channel_messages(
                client, entity, min_id=latest_id, offset_id=offset_id, session_name=session_name
            )

            if not batch:
                logger.info(
                    f"[{session_name}] No more messages to fetch for channel {channel.name}, stopped on ID: {offset_id}"
                )
                break

            new_messages = [m for m in batch if m.id not in processed_message_ids]

            if not new_messages:
                logger.info(f"[{session_name}] No new messages in this batch for channel {channel.name}")
                break

            # Update tracking variables
            processed_message_ids.update(m.id for m in new_messages)
            all_messages.extend(new_messages)

            # Save this batch of messages
            await self.save_channel_messages(client, channel, entity, new_messages, session_name)

            # Get the maximum ID in this batch to use as the next offset
            max_id_in_batch = max(m.id for m in batch)
            if max_id_in_batch == offset_id or len(batch) < self.batch_size:
                break
            offset_id = max_id_in_batch

            logger.info(
                f"[{session_name}] Fetched and processed {len(new_messages)} messages, "
                f"total so far: {len(all_messages)}, current ID: {offset_id}"
            )

        logger.info(
            f"[{session_name}] Total new messages processed for {channel.name}: {len(all_messages)}"
        )
        # Store all messages for later related channel processing
        self.processed_message_cache[channel.id] = all_messages

    async def _process_similar_channels(
            self, client, channel_repo, entity, channel_url, main_channel, session_name: str
    ):
        similar_channels_data = await self.get_similar_channels(
            client, entity, channel_url, session_name
        )
        logger.info(f"[{session_name}] Found {len(similar_channels_data)} similar channels")

        for similar_data in similar_channels_data:
            similar_channel = await channel_repo.get_or_create_channel(similar_data)
            if similar_channel:
                await channel_repo.add_similar_channel(main_channel, similar_channel)

    async def _process_related_channels(
            self, client, channel_repo, messages, main_channel, session_name: str
    ):
        """Process and save related channels from forwarded messages"""
        main_channel_id = getattr(main_channel, "channel_id", None)
        related_channel_ids = await self.extract_forwarded_channels(
            messages, main_channel_id, session_name
        )

        for related_id in related_channel_ids:
            related_info, related_entity = await self.get_channel_info_by_id(
                client, related_id, session_name
            )
            if related_info:
                related_channel = await channel_repo.get_or_create_channel(related_info)
                if related_channel:
                    await channel_repo.add_related_channel(
                        main_channel, related_channel
                    )
            else:
                logger.warning(
                    f"[{session_name}] Could not get info for related channel ID: {related_id}"
                )

        logger.info(f"[{session_name}] Added {len(related_channel_ids)} related channels")

    async def worker(self):
        """Worker that processes channels from the queue"""
        async with self.worker_lock:
            self.active_workers += 1
        
        worker_id = id(asyncio.current_task())
        worker_name = asyncio.current_task().get_name() if hasattr(asyncio.current_task(), "get_name") else f"worker-{worker_id}"
        logger.info(f"Worker {worker_name} started")
        
        try:
            while True:
                try:
                    # Check if any sessions are available
                    available_count = self.session_manager.get_available_session_count()
                    if available_count == 0:
                        logger.error(f"Worker {worker_name}: No available sessions left. Exiting.")
                        break
                        
                    # Get next channel to process with timeout
                    try:
                        channel_url = await asyncio.wait_for(self.channel_queue.get(), timeout=30)
                    except asyncio.TimeoutError:
                        logger.info(f"Worker {worker_name} timed out waiting for new channels, shutting down")
                        break
                    
                    logger.info(f"Worker {worker_name} processing channel: {channel_url}")
                    
                    # Try to process the channel until it succeeds
                    retry_count = 0
                    success = False
                    
                    while not success:
                        retry_count += 1
                        if retry_count > 1:
                            logger.info(f"Worker {worker_name} retry #{retry_count} for channel {channel_url}")
                        
                        success = await self.process_channel(channel_url)
                        
                        if not success:
                            # Check if we still have available sessions before retrying
                            available_count = self.session_manager.get_available_session_count()
                            if available_count == 0:
                                logger.error(f"Worker {worker_name}: No available sessions left. Putting channel back in queue.")
                                # Put the channel back in the queue for potential future retry when new sessions are available
                                self.channel_queue.put_nowait(channel_url)
                                self.channel_queue.task_done()  # Mark current attempt as done
                                break
                                
                            # Add a small delay before retrying with a different session
                            wait_time = min(retry_count * 2, 15)  # Exponential backoff up to 15 seconds
                            logger.info(f"Worker {worker_name} waiting {wait_time}s before retrying channel {channel_url}")
                            await asyncio.sleep(wait_time)
                    
                    # Mark task as done only if we succeeded
                    if success:
                        logger.info(f"Worker {worker_name} successfully processed channel {channel_url}")
                        self.channel_queue.task_done()
                    
                except Exception as e:
                    logger.error(f"Error in worker {worker_name}: {e}", exc_info=True)
                    # In case of unexpected error, try to mark the task as done to avoid deadlock
                    try:
                        self.channel_queue.task_done()
                    except:
                        pass
        finally:
            async with self.worker_lock:
                self.active_workers -= 1
            logger.info(f"Worker {worker_name} stopped")

    async def process_channels_by_category(self, categories: List[str]):
        try:
            async with async_session() as db_session:
                category_repo = CategoryRepository(db_session)

                # Get all channel URLs from all categories
                all_channels = []
                for category in categories:
                    logger.info(f"Fetching channels for category: {category}")
                    channel_urls = await category_repo.get_channel_urls_by_category(category)
                    logger.info(f"Found {len(channel_urls)} channels for category {category}")
                    all_channels.extend(channel_urls)
                
                # Filter out already processed channels
                channels_to_process = [url for url in all_channels if url not in self.processed_channels]
                logger.info(f"Total channels to process: {len(channels_to_process)}")
                
                # Add all channels to the queue
                for url in channels_to_process:
                    await self.channel_queue.put(url)
                
                # Start worker tasks
                workers = []
                logger.info(f"Starting {self.max_workers} workers to process channels...")
                for i in range(self.max_workers):
                    worker_task = asyncio.create_task(self.worker(), name=f"worker-{i+1}")
                    workers.append(worker_task)
                
                # Wait for queue to be processed
                logger.info(f"Waiting for all {len(channels_to_process)} channels to be processed...")
                await self.channel_queue.join()
                logger.info("All channels have been processed!")
                
                # Cancel any active workers
                active_workers = sum(1 for w in workers if not w.done())
                logger.info(f"Stopping {active_workers} active workers...")
                for worker in workers:
                    worker.cancel()
                
                # Wait for all workers to finish
                await asyncio.gather(*workers, return_exceptions=True)
                logger.info("All workers have stopped.")
                
                available_sessions = self.session_manager.get_available_session_count()
                total_sessions = len(self.session_manager.session_files)
                banned_sessions = len(self.session_manager.banned_sessions)
                
                logger.info(f"Session stats: {available_sessions}/{total_sessions} available (banned: {banned_sessions})")
                logger.info(f"Completed processing all categories")
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
