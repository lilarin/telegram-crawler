from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import logger
from app.core.models import Channel, ChannelSimilar, Category, Link, CategoryLink


class ChannelRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[datetime.date]:
        """Parse date string in format DD.MM.YYYY into a date object"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%d.%m.%Y').date()
        except ValueError:
            logger.error(f"Invalid date format: {date_str}, expected DD.MM.YYYY")
            return None

    async def get_channel_by_link(self, link: str) -> Optional[Channel]:
        """Get a channel by its link"""
        try:
            result = await self.session.execute(
                select(Channel).where(Channel.link == link)
            )
            return result.scalars().first()
        except Exception as e:
            logger.error(f"Error getting channel by link {link}: {e}")
            return None

    async def get_or_create_channel(self, channel_data: Dict) -> Optional[Channel]:
        """Get or create a channel from provided data"""
        try:
            link = channel_data.get("link")
            if not link:
                logger.error("No link provided for channel")
                return None

            # Parse the created_at date from string to datetime.date object
            created_at_str = channel_data.get("created_at")
            created_at_date = self.parse_date(created_at_str)

            # Check if channel already exists
            channel = await self.get_channel_by_link(link)
            if channel:
                # Update existing channel with new data
                channel.name = channel_data.get("name", channel.name)
                channel.channel_id = channel_data.get("id", channel.channel_id)
                channel.subscribers = channel_data.get("subscribers", channel.subscribers)
                channel.verified = channel_data.get("verified", channel.verified)
                channel.created_at = created_at_date
            else:
                # Create new channel
                channel = Channel(
                    channel_id=channel_data.get("id"),
                    name=channel_data.get("name"),
                    link=link,
                    subscribers=channel_data.get("subscribers"),
                    verified=channel_data.get("verified", False),
                    created_at=created_at_date
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

    async def add_similar_channel(self, main_channel: Channel, similar_channel: Channel) -> bool:
        """Create a relationship between a main channel and a similar channel"""
        try:
            # Check if relationship already exists
            query = select(ChannelSimilar).where(
                ChannelSimilar.main_channel_id == main_channel.id,
                ChannelSimilar.similar_channel_id == similar_channel.id
            )
            result = await self.session.execute(query)
            if result.scalars().first():
                return True  # Relationship already exists

            # Create new relationship
            similar_relation = ChannelSimilar(
                main_channel_id=main_channel.id,
                similar_channel_id=similar_channel.id
            )
            self.session.add(similar_relation)
            await self.session.commit()
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error adding similar channel relationship: {e}")
            return False

    async def get_channels_by_category(self, category_name: str) -> List[Channel]:
        """Get all channels that belong to a specific category"""
        try:
            # First, get all links associated with this category
            query = (
                select(Link)
                .join(CategoryLink)
                .join(Category)
                .where(Category.name == category_name)
            )
            result = await self.session.execute(query)
            links = result.scalars().all()
            
            # Then, get all channels associated with these links
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
        """Get a channel by its ID"""
        try:
            result = await self.session.execute(
                select(Channel).where(Channel.id == channel_id)
            )
            return result.scalars().first()
        except Exception as e:
            logger.error(f"Error getting channel by ID {channel_id}: {e}")
            return None

    async def get_all_channels_with_similar(self) -> List[Dict]:
        """Get all channels with their similar channels"""
        try:
            # Get all channels
            query = select(Channel)
            result = await self.session.execute(query)
            channels = result.scalars().all()
            
            # Format the results
            channels_with_similar = []
            for channel in channels:
                # Get similar channels for this channel
                similar_query = (
                    select(Channel)
                    .join(ChannelSimilar, Channel.id == ChannelSimilar.similar_channel_id)
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
                        "created_at": similar.created_at.strftime('%d.%m.%Y') if similar.created_at else None
                    }
                    similar_channels_data.append(similar_data)
                
                # Only include channels that have similar channels or are similar to others
                if similar_channels_data:
                    channel_data = {
                        "id": channel.channel_id,
                        "name": channel.name,
                        "link": channel.link,
                        "subscribers": channel.subscribers,
                        "verified": channel.verified,
                        "created_at": channel.created_at.strftime('%d.%m.%Y') if channel.created_at else None,
                        "similar_channels": similar_channels_data
                    }
                    channels_with_similar.append(channel_data)
                
            return channels_with_similar
            
        except Exception as e:
            logger.error(f"Error getting all channels with similar: {e}")
            return []
            
    async def get_channels_by_category_with_similar(self, category_name: str) -> List[Dict]:
        """Get all channels for a specific category with their similar channels"""
        try:
            # Get all channels for this category
            category_channels = await self.get_channels_by_category(category_name)
            
            result = []
            for channel in category_channels:
                # Get the similar channels for this channel
                query = (
                    select(Channel)
                    .join(ChannelSimilar, Channel.id == ChannelSimilar.similar_channel_id)
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
                        "created_at": similar.created_at.strftime('%d.%m.%Y') if similar.created_at else None
                    }
                    for similar in similar_channels
                ]
                
                result.append({
                    "id": channel.channel_id,
                    "name": channel.name,
                    "link": channel.link,
                    "subscribers": channel.subscribers,
                    "verified": channel.verified,
                    "created_at": channel.created_at.strftime('%d.%m.%Y') if channel.created_at else None,
                    "similar_channels": similar_channels_data
                })
                
            return result
            
        except Exception as e:
            logger.error(f"Error getting channels for category {category_name} with similar: {e}")
            return [] 