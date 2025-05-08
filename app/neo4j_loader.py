import asyncio
import sys

from app.config import logger, config
from app.core.database import async_session
from app.repositories.channel_repository import ChannelRepository
from app.repositories.category_repository import CategoryRepository
from app.repositories.neo4j_repository import Neo4jManager


async def load_channel_data_from_db(neo4j_uri, neo4j_user, neo4j_password, clear_db=True):
    try:
        # Connect to Neo4j
        neo4j_manager = Neo4jManager(
            uri=neo4j_uri,
            username=neo4j_user,
            password=neo4j_password
        )
        
        if not neo4j_manager.driver:
            logger.error("Failed to initialize Neo4j manager")
            return False

        # Clear the database before importing new data if requested
        if clear_db:
            logger.info("Clearing database before import...")
            if not neo4j_manager.clear_database():
                logger.error("Failed to clear database")
                neo4j_manager.close()
                return False
            logger.info("Database cleared successfully")

        # Get channels data from SQL database
        async with async_session() as db_session:
            channel_repo = ChannelRepository(db_session)
            category_repo = CategoryRepository(db_session)
            
            # Get all channels by category
            categories_with_channels = await category_repo.get_all_categories_with_channels()
            
            # Get all channels with their similar channels
            channels_with_similar = await channel_repo.get_all_channels_with_similar()
            
            # Enhance categories_with_channels with similar channels information
            for category, channels in categories_with_channels.items():
                for channel in channels:
                    # Find this channel in channels_with_similar
                    for channel_with_similar in channels_with_similar:
                        if channel_with_similar.get("link") == channel.get("link"):
                            channel["similar_channels"] = channel_with_similar.get("similar_channels", [])
                            break
                    
                    # If we didn't find it, ensure it has an empty similar_channels list
                    if "similar_channels" not in channel:
                        channel["similar_channels"] = []
            
            total_categories = len(categories_with_channels)
            total_channels = sum(len(channels) for channels in categories_with_channels.values())
            logger.info(f"Retrieved {total_channels} channels across {total_categories} categories from database")

            # Import channels to Neo4j
            success = neo4j_manager.import_channels_data(categories_with_channels)
            
            neo4j_manager.close()
            
            if success:
                logger.info(f"Successfully imported {total_channels} channels across {total_categories} categories to Neo4j")
            
            return success
            
    except Exception as e:
        logger.error(f"Error loading channel data from database: {e}")
        return False


async def main():
    logger.info(f"Loading channel data to Neo4j from database...")
    
    success = await load_channel_data_from_db(
        neo4j_uri=config.NEO4J_URI,
        neo4j_user=config.NEO4J_USER,
        neo4j_password=config.NEO4J_PASSWORD,
        clear_db=True
    )

    if success:
        logger.info("Successfully loaded channel data to Neo4j")
    else:
        logger.error("Failed to load channel data to Neo4j")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
