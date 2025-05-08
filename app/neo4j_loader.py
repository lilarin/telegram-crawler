import asyncio
import json
import os
import sys

from app.config import logger, config
from app.repositories.neo4j_repository import Neo4jManager


async def load_channel_data(channels_file, neo4j_uri, neo4j_user, neo4j_password, clear_db=True):
    if not os.path.exists(channels_file):
        logger.error(f"File not found: {channels_file}")
        return False

    try:
        with open(channels_file, 'r', encoding='utf-8') as f:
            channels_data = json.load(f)

        logger.info(f"Loaded channel data from {channels_file}")
        logger.info(f"Found {len(channels_data)} categories")
        
        total_channels = sum(len(channels) for channels in channels_data.values())
        logger.info(f"Total channels to process: {total_channels}")

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

        success = neo4j_manager.import_channels_data(channels_data)

        neo4j_manager.close()

        if success:
            logger.info(f"Successfully imported {total_channels} channels across {len(channels_data)} categories")
        
        return success
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON file: {e}")
        return False
    except Exception as e:
        logger.error(f"Error loading channel data: {e}")
        return False


async def main():
    logger.info(f"Loading channel data to Neo4j...")

    logger.info(f"Using channels file: {config.SIMILAR_CHANNELS_FILE}")
    
    success = await load_channel_data(
        channels_file=config.SIMILAR_CHANNELS_FILE,
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
