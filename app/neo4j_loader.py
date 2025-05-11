import asyncio

from app.config import logger, config
from app.core.database import async_session
from app.repositories.category_repository import CategoryRepository
from app.repositories.channel_repository import ChannelRepository
from app.repositories.neo4j_repository import Neo4jManager


async def load_channel_data_from_db(
        neo4j_uri, neo4j_user, neo4j_password, clear_db=True
):
    try:
        neo4j_manager = Neo4jManager(
            uri=neo4j_uri, username=neo4j_user, password=neo4j_password
        )

        if not neo4j_manager.driver:
            logger.error("Failed to initialize Neo4j manager")
            return False

        if clear_db:
            logger.info("Clearing database before import...")
            if not neo4j_manager.clear_database():
                logger.error("Failed to clear database")
                neo4j_manager.close()
                return False
            logger.info("Database cleared successfully")

        async with async_session() as db_session:
            channel_repo = ChannelRepository(db_session)
            category_repo = CategoryRepository(db_session)

            categories_with_channels = (
                await category_repo.get_all_categories_with_channels()
            )

            channels_with_similar = await channel_repo.get_all_channels_with_similar()

            channels_with_related = await channel_repo.get_all_channels_with_related()

            for category, channels in categories_with_channels.items():
                for channel in channels:
                    similar_channels = []
                    for ch_with_similar in channels_with_similar:
                        if ch_with_similar.get("link") == channel.get("link"):
                            similar_channels = ch_with_similar.get(
                                "similar_channels", []
                            )
                            break

                    channel["similar_channels"] = similar_channels

                    related_channels = []
                    for ch_with_related in channels_with_related:
                        if ch_with_related.get("link") == channel.get("link"):
                            related_channels = ch_with_related.get(
                                "related_channels", []
                            )
                            break

                    channel["related_channels"] = related_channels

            total_categories = len(categories_with_channels)
            total_channels = sum(
                len(channels) for channels in categories_with_channels.values()
            )
            logger.info(
                f"Retrieved {total_channels} channels across "
                f"{total_categories} categories from database"
            )

            success = neo4j_manager.import_channels_data(categories_with_channels)

            neo4j_manager.close()

            if success:
                logger.info(
                    f"Successfully imported {total_channels} "
                    f"channels across {total_categories} categories to Neo4j"
                )

            return success

    except Exception as e:
        logger.error(f"Error loading channel data from database: {e}")
        return False


async def main():
    logger.info(f"Loading channel data to Neo4j from database...")

    await load_channel_data_from_db(
        neo4j_uri=config.NEO4J_URI,
        neo4j_user=config.NEO4J_USER,
        neo4j_password=config.NEO4J_PASSWORD,
        clear_db=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
