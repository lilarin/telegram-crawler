from typing import Dict, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.models import Category, Link, CategoryLink


class CategoryRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all_categories(self) -> List[Category]:
        """Get all categories from database"""
        stmt = select(Category)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_all_channels_by_category(self) -> Dict[str, List[str]]:
        """Get all channels grouped by category"""
        channels_by_category = {}

        # Get all categories
        categories = await self.get_all_categories()

        # For each category, get all links
        for category in categories:
            stmt = select(Link).join(CategoryLink).where(
                CategoryLink.category_id == category.id
            )
            result = await self.session.execute(stmt)
            links = result.scalars().all()

            # Add links to the dictionary
            if links:
                channels_by_category[category.name] = [link.url for link in links]

        return channels_by_category

    async def get_channels_by_category_name(self, category_name: str) -> List[str]:
        """Get all channels for a specific category"""
        stmt = select(Category).where(Category.name == category_name)
        result = await self.session.execute(stmt)
        category = result.scalar_one_or_none()

        if not category:
            return []

        stmt = select(Link).join(CategoryLink).where(
            CategoryLink.category_id == category.id
        )
        result = await self.session.execute(stmt)
        links = result.scalars().all()

        return [link.url for link in links]
