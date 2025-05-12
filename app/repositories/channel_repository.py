from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import logger
from app.core.models import (
    Channel,
    ChannelSimilar,
    ChannelRelated,
    Category,
    Link,
    CategoryLink,
    ChannelMessage,
)


class ChannelRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[datetime.date]:
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%d.%m.%Y").date()
        except ValueError:
            logger.error(f"Invalid date format: {date_str}, expected DD.MM.YYYY")
            return None

    async def get_channel_by_link(self, link: str) -> Optional[Channel]:
        try:
            result = await self.session.execute(
                select(Channel).where(Channel.link == link)
            )
            return result.scalars().first()
        except Exception as e:
            logger.error(f"Error getting channel by link {link}: {e}")
            return None

    async def get_or_create_channel(self, channel_data: Dict) -> Optional[Channel]:
        try:
            link = channel_data.get("link")
            if not link:
                logger.error("No link provided for channel")
                return None

            created_at_str = channel_data.get("created_at")
            created_at_date = self.parse_date(created_at_str)

            channel = await self.get_channel_by_link(link)
            if channel:
                channel.name = channel_data.get("name", channel.name)
                channel.channel_id = channel_data.get("id", channel.channel_id)
                channel.subscribers = channel_data.get(
                    "subscribers", channel.subscribers
                )
                channel.verified = channel_data.get("verified", channel.verified)
                channel.created_at = created_at_date
            else:
                channel = Channel(
                    channel_id=channel_data.get("id"),
                    name=channel_data.get("name"),
                    link=link,
                    subscribers=channel_data.get("subscribers"),
                    verified=channel_data.get("verified", False),
                    created_at=created_at_date,
                )
                self.session.add(channel)

            await self.session.commit()
            await self.session.refresh(channel)
            return channel

        except IntegrityError as e:
            await self.session.rollback()
            logger.error(f"Integrity error creating/updating channel: {e}")
            return None
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error creating/updating channel: {e}")
            return None

    async def add_similar_channel(
        self, main_channel: Channel, similar_channel: Channel
    ) -> bool:
        try:
            query = select(ChannelSimilar).where(
                ChannelSimilar.main_channel_id == main_channel.id,
                ChannelSimilar.similar_channel_id == similar_channel.id,
            )
            result = await self.session.execute(query)

            if result.scalars().first():
                return True

            similar_relation = ChannelSimilar(
                main_channel_id=main_channel.id, similar_channel_id=similar_channel.id
            )
            self.session.add(similar_relation)
            await self.session.commit()
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error adding similar channel relationship: {e}")
            return False

    async def add_related_channel(
        self, main_channel: Channel, related_channel: Channel
    ) -> bool:
        try:
            query = select(ChannelRelated).where(
                ChannelRelated.main_channel_id == main_channel.id,
                ChannelRelated.related_channel_id == related_channel.id,
            )
            result = await self.session.execute(query)
            if result.scalars().first():
                return True

            related_relation = ChannelRelated(
                main_channel_id=main_channel.id, related_channel_id=related_channel.id
            )
            self.session.add(related_relation)
            await self.session.commit()
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error adding related channel relationship: {e}")
            return False

    async def get_channels_by_category(self, category_name: str) -> List[Channel]:
        try:
            query = (
                select(Link)
                .join(CategoryLink)
                .join(Category)
                .where(Category.name == category_name)
            )
            result = await self.session.execute(query)
            links = result.scalars().all()

            channels = []
            for link in links:
                channel_query = select(Channel).where(Channel.link == link.url)
                channel_result = await self.session.execute(channel_query)
                channel = channel_result.scalars().first()
                if channel:
                    channels.append(channel)

            return channels
        except Exception as e:
            logger.error(f"Error getting channels for category {category_name}: {e}")
            return []

    async def get_channel_by_id(self, channel_id: int) -> Optional[Channel]:
        try:
            result = await self.session.execute(
                select(Channel).where(Channel.id == channel_id)
            )
            return result.scalars().first()
        except Exception as e:
            logger.error(f"Error getting channel by ID {channel_id}: {e}")
            return None

    async def get_all_channels_with_similar(self) -> List[Dict]:
        try:
            query = select(Channel)
            result = await self.session.execute(query)
            channels = result.scalars().all()

            channels_with_similar = []
            for channel in channels:
                similar_query = (
                    select(Channel)
                    .join(
                        ChannelSimilar, Channel.id == ChannelSimilar.similar_channel_id
                    )
                    .where(ChannelSimilar.main_channel_id == channel.id)
                )
                similar_result = await self.session.execute(similar_query)
                similar_channels = similar_result.scalars().all()

                similar_channels_data = []
                for similar in similar_channels:
                    similar_data = {
                        "id": similar.channel_id,
                        "name": similar.name,
                        "link": similar.link,
                        "subscribers": similar.subscribers,
                        "verified": similar.verified,
                        "created_at": (
                            similar.created_at.strftime("%d.%m.%Y")
                            if similar.created_at
                            else None
                        ),
                    }
                    similar_channels_data.append(similar_data)

                if similar_channels_data:
                    channel_data = {
                        "id": channel.channel_id,
                        "name": channel.name,
                        "link": channel.link,
                        "subscribers": channel.subscribers,
                        "verified": channel.verified,
                        "created_at": (
                            channel.created_at.strftime("%d.%m.%Y")
                            if channel.created_at
                            else None
                        ),
                        "similar_channels": similar_channels_data,
                    }
                    channels_with_similar.append(channel_data)

            return channels_with_similar

        except Exception as e:
            logger.error(f"Error getting all channels with similar: {e}")
            return []

    async def get_all_channels_with_related(self) -> List[Dict]:
        try:
            query = select(Channel)
            result = await self.session.execute(query)
            channels = result.scalars().all()

            channels_with_related = []
            for channel in channels:
                related_query = (
                    select(Channel)
                    .join(
                        ChannelRelated, Channel.id == ChannelRelated.related_channel_id
                    )
                    .where(ChannelRelated.main_channel_id == channel.id)
                )
                related_result = await self.session.execute(related_query)
                related_channels = related_result.scalars().all()

                related_channels_data = []
                for related in related_channels:
                    related_data = {
                        "id": related.channel_id,
                        "name": related.name,
                        "link": related.link,
                        "subscribers": related.subscribers,
                        "verified": related.verified,
                        "created_at": (
                            related.created_at.strftime("%d.%m.%Y")
                            if related.created_at
                            else None
                        ),
                    }
                    related_channels_data.append(related_data)

                if related_channels_data:
                    channel_data = {
                        "id": channel.channel_id,
                        "name": channel.name,
                        "link": channel.link,
                        "subscribers": channel.subscribers,
                        "verified": channel.verified,
                        "created_at": (
                            channel.created_at.strftime("%d.%m.%Y")
                            if channel.created_at
                            else None
                        ),
                        "related_channels": related_channels_data,
                    }
                    channels_with_related.append(channel_data)

            return channels_with_related

        except Exception as e:
            logger.error(f"Error getting all channels with related: {e}")
            return []

    async def get_channels_by_category_with_similar(
        self, category_name: str
    ) -> List[Dict]:
        try:
            category_channels = await self.get_channels_by_category(category_name)

            result = []
            for channel in category_channels:
                query = (
                    select(Channel)
                    .join(
                        ChannelSimilar, Channel.id == ChannelSimilar.similar_channel_id
                    )
                    .where(ChannelSimilar.main_channel_id == channel.id)
                )
                similar_result = await self.session.execute(query)
                similar_channels = similar_result.scalars().all()

                similar_channels_data = [
                    {
                        "id": similar.channel_id,
                        "name": similar.name,
                        "link": similar.link,
                        "subscribers": similar.subscribers,
                        "verified": similar.verified,
                        "created_at": (
                            similar.created_at.strftime("%d.%m.%Y")
                            if similar.created_at
                            else None
                        ),
                    }
                    for similar in similar_channels
                ]

                result.append(
                    {
                        "id": channel.channel_id,
                        "name": channel.name,
                        "link": channel.link,
                        "subscribers": channel.subscribers,
                        "verified": channel.verified,
                        "created_at": (
                            channel.created_at.strftime("%d.%m.%Y")
                            if channel.created_at
                            else None
                        ),
                        "similar_channels": similar_channels_data,
                    }
                )

            return result

        except Exception as e:
            logger.error(
                f"Error getting channels for category {category_name} with similar: {e}"
            )
            return []

    async def save_channel_message(
        self, channel_id: int, message_id: int, message_data: Dict
    ) -> Optional[ChannelMessage]:
        try:
            query = select(ChannelMessage).where(
                ChannelMessage.channel_id == channel_id,
                ChannelMessage.message_id == message_id,
            )
            result = await self.session.execute(query)
            existing_message = result.scalars().first()

            if existing_message:
                existing_message.data = message_data
            else:
                new_message = ChannelMessage(
                    channel_id=channel_id, message_id=message_id, data=message_data
                )
                self.session.add(new_message)

            await self.session.commit()

            query = select(ChannelMessage).where(
                ChannelMessage.channel_id == channel_id,
                ChannelMessage.message_id == message_id,
            )
            result = await self.session.execute(query)
            return result.scalars().first()

        except Exception as e:
            await self.session.rollback()
            logger.error(
                f"Error saving message {message_id} for channel {channel_id}: {e}"
            )
            return None

    async def get_channel_messages(
        self, channel_id: int, limit: int = 100, offset: int = 0
    ) -> List[Dict]:
        try:
            query = (
                select(ChannelMessage)
                .where(ChannelMessage.channel_id == channel_id)
                .order_by(ChannelMessage.id.desc())
                .offset(offset)
                .limit(limit)
            )
            result = await self.session.execute(query)
            messages = result.scalars().all()

            return [
                {
                    "id": message.id,
                    "message_id": message.message_id,
                    "data": message.data,
                }
                for message in messages
            ]

        except Exception as e:
            logger.error(f"Error getting messages for channel {channel_id}: {e}")
            return []

    async def get_latest_message_id(self, channel_id: int) -> Optional[int]:
        try:
            query = (
                select(ChannelMessage.message_id)
                .where(ChannelMessage.channel_id == channel_id)
                .order_by(ChannelMessage.message_id.desc())
                .limit(1)
            )
            result = await self.session.execute(query)
            latest_message = result.scalar_one_or_none()
            return latest_message
        except Exception as e:
            logger.error(
                f"Error getting latest message ID for channel {channel_id}: {e}"
            )
            return None
