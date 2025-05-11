from typing import Dict, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import logger
from app.core.models import Category, Link, CategoryLink, Channel


class CategoryRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all_categories(self) -> List[Category]:
        stmt = select(Category)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_all_channels_by_category(self) -> Dict[str, List[str]]:
        channels_by_category = {}

        categories = await self.get_all_categories()

        for category in categories:
            stmt = (
                select(Link)
                .join(CategoryLink)
                .where(CategoryLink.category_id == category.id)
            )
            result = await self.session.execute(stmt)
            links = result.scalars().all()

            if links:
                channels_by_category[category.name] = [link.url for link in links]

        return channels_by_category

    async def get_channel_urls_by_category(self, category_name: str) -> List[str]:
        stmt = select(Category).where(Category.name == category_name)
        result = await self.session.execute(stmt)
        category = result.scalar_one_or_none()

        if not category:
            return []

        stmt = (
            select(Link)
            .join(CategoryLink)
            .where(CategoryLink.category_id == category.id)
        )
        result = await self.session.execute(stmt)
        links = result.scalars().all()

        return [link.url for link in links]

    async def get_all_category_names(self) -> List[str]:
        stmt = select(Category.name)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_all_categories_with_channels(self) -> Dict[str, List[Dict]]:
        categories_with_channels = {}

        try:
            categories = await self.get_all_categories()

            for category in categories:
                links_query = (
                    select(Link)
                    .join(CategoryLink)
                    .where(CategoryLink.category_id == category.id)
                )
                links_result = await self.session.execute(links_query)
                links = links_result.scalars().all()

                if not links:
                    continue

                channel_list = []
                for link in links:
                    channel_query = select(Channel).where(Channel.link == link.url)
                    channel_result = await self.session.execute(channel_query)
                    channel = channel_result.scalars().first()

                    if channel:
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
                        }
                        channel_list.append(channel_data)

                if channel_list:
                    categories_with_channels[category.name] = channel_list

            return categories_with_channels

        except Exception as e:
            logger.error(f"Error getting categories with channels: {e}")
            return {}
