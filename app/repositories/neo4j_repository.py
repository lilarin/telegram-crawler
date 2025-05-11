from neo4j import GraphDatabase

from app.config import logger


class Neo4jManager:
    def __init__(self, uri, username, password):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            logger.info("Successfully connected to Neo4j database")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            self.driver = None

    def close(self):
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")

    def clear_database(self):
        query = """
        MATCH (n)
        DETACH DELETE n
        """

        try:
            with self.driver.session() as session:
                session.run(query)
                logger.info("Database cleared successfully")
                return True
        except Exception as e:
            logger.error(f"Error clearing database: {e}")
            return False

    def create_channel_node(self, channel_data):
        channel_id = (
            str(channel_data.get("id"))
            if channel_data.get("id")
            else channel_data.get("link")
        )

        query = """
        MERGE (c:Channel {id: $id}) 
        ON CREATE SET 
            c.name = $name, 
            c.link = $link, 
            c.subscribers = $subscribers,
            c.verified = $verified,
            c.created_at = $created_at_date,
            c.system_created_at = timestamp()
        ON MATCH SET 
            c.name = $name, 
            c.link = $link,
            c.subscribers = $subscribers,
            c.verified = $verified,
            c.created_at = $created_at_date,
            c.system_updated_at = timestamp()
        RETURN c
        """

        try:
            with self.driver.session() as session:
                result = session.run(
                    query,
                    id=channel_id,
                    name=channel_data.get("name", ""),
                    link=channel_data.get("link", ""),
                    subscribers=channel_data.get("subscribers", 0),
                    verified=channel_data.get("verified", False),
                    created_at_date=channel_data.get("created_at", ""),
                )
                record = result.single()
                return record is not None
        except Exception as e:
            logger.error(f"Error creating channel node: {e}")
            return False

    def create_similar_channel_relationship(self, source_channel, similar_channel):
        source_id = (
            str(source_channel.get("id"))
            if source_channel.get("id")
            else source_channel.get("link")
        )
        similar_id = (
            str(similar_channel.get("id"))
            if similar_channel.get("id")
            else similar_channel.get("link")
        )

        query = """
        MATCH (source:Channel {id: $source_id})
        MATCH (similar:Channel {id: $similar_id})
        MERGE (source)-[r:SIMILAR_TO]->(similar)
        ON CREATE SET r.created_at = timestamp()
        RETURN r
        """

        try:
            with self.driver.session() as session:
                result = session.run(query, source_id=source_id, similar_id=similar_id)
                record = result.single()
                return record is not None
        except Exception as e:
            logger.error(f"Error creating similar relationship: {e}")
            return False

    def create_related_channel_relationship(self, source_channel, related_channel):
        source_id = (
            str(source_channel.get("id"))
            if source_channel.get("id")
            else source_channel.get("link")
        )
        related_id = (
            str(related_channel.get("id"))
            if related_channel.get("id")
            else related_channel.get("link")
        )

        query = """
        MATCH (source:Channel {id: $source_id})
        MATCH (related:Channel {id: $related_id})
        MERGE (source)-[r:REPOSTS_FROM]->(related)
        ON CREATE SET r.created_at = timestamp()
        RETURN r
        """

        try:
            with self.driver.session() as session:
                result = session.run(query, source_id=source_id, related_id=related_id)
                record = result.single()
                return record is not None
        except Exception as e:
            logger.error(f"Error creating related relationship: {e}")
            return False

    def add_category_to_channel(self, channel_data, category):
        channel_id = (
            str(channel_data.get("id"))
            if channel_data.get("id")
            else channel_data.get("link")
        )

        category_label = "".join(x for x in category.title() if x.isalnum())

        query = f"""
        MATCH (c:Channel {{id: $id}})
        SET c:{category_label}
        SET c.category = CASE
            WHEN c.category IS NULL THEN $category
            WHEN NOT c.category CONTAINS $category THEN c.category + ',' + $category
            ELSE c.category
        END
        RETURN c
        """

        try:
            with self.driver.session() as session:
                result = session.run(query, id=channel_id, category=category)
                record = result.single()
                return record is not None
        except Exception as e:
            logger.error(f"Error adding category to channel: {e}")
            return False

    def import_channels_data(self, channels_data):
        try:
            for category, channels in channels_data.items():
                logger.info(
                    f"Processing category: {category} with {len(channels)} channels"
                )

                for channel in channels:
                    success = self.create_channel_node(channel)
                    if success:
                        self.add_category_to_channel(channel, category)

                        # Process similar channels
                        if (
                            "similar_channels" in channel
                            and channel["similar_channels"]
                        ):
                            for similar_channel in channel["similar_channels"]:
                                self.create_channel_node(similar_channel)
                                self.add_category_to_channel(similar_channel, category)
                                self.create_similar_channel_relationship(channel, similar_channel)

                        # Process related channels
                        if (
                            "related_channels" in channel
                            and channel["related_channels"]
                        ):
                            for related_channel in channel["related_channels"]:
                                self.create_channel_node(related_channel)
                                self.create_related_channel_relationship(channel, related_channel)

            return True
        except Exception as e:
            logger.error(f"Error importing channels data: {e}")
            return False
