from typing import List

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import logger
from app.core.models import Category, Link, CategoryLink


class TGStatRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_or_create_category(self, category_name: str) -> Category:
        result = await self.session.execute(
            select(Category).where(Category.name == category_name)
        )
        category = result.scalar_one_or_none()

        if not category:
            category = Category(name=category_name)
            self.session.add(category)
            await self.session.flush()

        return category

    async def get_or_create_link(self, link_url: str) -> Link:
        result = await self.session.execute(select(Link).where(Link.url == link_url))
        link = result.scalar_one_or_none()

        if not link:
            link = Link(url=link_url)
            self.session.add(link)
            await self.session.flush()

        return link

    async def check_link_category_association(self, category_id: int, link_id: int) -> bool:
        stmt = (
            select(Category)
            .join(CategoryLink)
            .where(and_(Category.id == category_id, CategoryLink.link_id == link_id))
        )
        result = await self.session.execute(stmt)
        existing_category = result.scalar_one_or_none()

        return existing_category is not None

    async def add_link_to_category(self, category_id: int, link_id: int) -> bool:
        try:
            if await self.check_link_category_association(category_id, link_id):
                return False

            category_link = CategoryLink(category_id=category_id, link_id=link_id)
            self.session.add(category_link)
            await self.session.flush()
            return True
        except Exception as e:
            logger.error(f"Error adding link to category: {e}")
            return False

    async def save_channels_for_category(
        self, category_name: str, channel_urls: List[str]
    ) -> bool:
        try:
            category = await self.get_or_create_category(category_name)
            logger.info(f"Category created/found: {category.name} (id={category.id})")

            for url in channel_urls:
                link = await self.get_or_create_link(url)
                logger.info(f"Link created/found: {link.url} (id={link.id})")

                added = await self.add_link_to_category(category.id, link.id)
                if added:
                    logger.info(f"Added category {category_name} to link {url}")

            await self.session.commit()
            logger.info(
                f"Successfully saved {len(channel_urls)} "
                f"channels for category {category_name}"
            )
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error saving channels for category: {e}")
            return False
