from sqlalchemy import Column, Integer, String, \
    ForeignKey  # Table больше не нужен для определения *отдельной* ассоциативной таблицы
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from app.core.database import Base


class CategoryLink(Base):
    __tablename__ = 'category_links'

    category_id = Column(Integer, ForeignKey('categories.id', ondelete='CASCADE'), primary_key=True)
    link_id = Column(Integer, ForeignKey('links.id', ondelete='CASCADE'), primary_key=True)
    category = relationship("Category", back_populates="link_associations")
    link = relationship("Link", back_populates="category_associations")


class Category(Base):
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    link_associations = relationship(
        "CategoryLink",
        back_populates="category",
        cascade="all, delete-orphan"
    )
    links = association_proxy("link_associations", "link")


# 3. Модифицируем модель Link
class Link(Base):
    __tablename__ = 'links'

    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, nullable=False)

    category_associations = relationship(
        "CategoryLink",
        back_populates="link",
        cascade="all, delete-orphan"
    )
    categories = association_proxy("category_associations", "category")
