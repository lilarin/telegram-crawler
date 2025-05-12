from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Boolean,
    Date,
    BigInteger,
    JSON,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from app.core.database import Base


class CategoryLink(Base):
    __tablename__ = "category_links"

    category_id = Column(
        Integer,
        ForeignKey(column="categories.id", ondelete="CASCADE"),
        primary_key=True,
    )
    link_id = Column(
        Integer, ForeignKey(column="links.id", ondelete="CASCADE"), primary_key=True
    )
    category = relationship(argument="Category", back_populates="link_associations")
    link = relationship(argument="Link", back_populates="category_associations")


class Category(Base):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    link_associations = relationship(
        "CategoryLink", back_populates="category", cascade="all, delete-orphan"
    )
    links = association_proxy(target_collection="link_associations", attr="link")


class Link(Base):
    __tablename__ = "links"

    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, nullable=False)

    category_associations = relationship(
        "CategoryLink", back_populates="link", cascade="all, delete-orphan"
    )
    categories = association_proxy(
        target_collection="category_associations", attr="category"
    )


class Channel(Base):
    __tablename__ = "channels"

    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger, unique=True, nullable=True)
    name = Column(String(255), nullable=True)
    link = Column(String(255), unique=True, nullable=False)
    subscribers = Column(Integer, nullable=True)
    verified = Column(Boolean, default=False)
    created_at = Column(Date, nullable=True)

    similar_to = relationship(
        "ChannelSimilar",
        foreign_keys="ChannelSimilar.main_channel_id",
        back_populates="main_channel",
        cascade="all, delete-orphan",
    )
    similar_by = relationship(
        "ChannelSimilar",
        foreign_keys="ChannelSimilar.similar_channel_id",
        back_populates="similar_channel",
        cascade="all, delete-orphan",
    )

    related_to = relationship(
        "ChannelRelated",
        foreign_keys="ChannelRelated.main_channel_id",
        back_populates="main_channel",
        cascade="all, delete-orphan",
    )
    related_by = relationship(
        "ChannelRelated",
        foreign_keys="ChannelRelated.related_channel_id",
        back_populates="related_channel",
        cascade="all, delete-orphan",
    )

    messages = relationship(
        "ChannelMessage", back_populates="channel", cascade="all, delete-orphan"
    )


class ChannelSimilar(Base):
    __tablename__ = "channel_similar"

    main_channel_id = Column(
        Integer, ForeignKey(column="channels.id", ondelete="CASCADE"), primary_key=True
    )
    similar_channel_id = Column(
        Integer, ForeignKey(column="channels.id", ondelete="CASCADE"), primary_key=True
    )

    main_channel = relationship(
        "Channel", foreign_keys=[main_channel_id], back_populates="similar_to"
    )
    similar_channel = relationship(
        "Channel", foreign_keys=[similar_channel_id], back_populates="similar_by"
    )


class ChannelRelated(Base):
    __tablename__ = "channel_related"

    main_channel_id = Column(
        Integer, ForeignKey(column="channels.id", ondelete="CASCADE"), primary_key=True
    )
    related_channel_id = Column(
        Integer, ForeignKey(column="channels.id", ondelete="CASCADE"), primary_key=True
    )

    main_channel = relationship(
        "Channel", foreign_keys=[main_channel_id], back_populates="related_to"
    )
    related_channel = relationship(
        "Channel", foreign_keys=[related_channel_id], back_populates="related_by"
    )


class ChannelMessage(Base):
    __tablename__ = "channel_messages"

    id = Column(Integer, primary_key=True)
    channel_id = Column(
        Integer, ForeignKey("channels.id", ondelete="CASCADE"), nullable=False
    )
    message_id = Column(BigInteger, nullable=False)
    data = Column(JSON, nullable=False)

    channel = relationship("Channel", back_populates="messages")
