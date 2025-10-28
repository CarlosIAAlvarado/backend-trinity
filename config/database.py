# ==========================
# Database Configuration
# ==========================
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
import os
import logging

logger = logging.getLogger(__name__)

class DatabaseConfig:
    """
    Database configuration following Single Responsibility Principle
    Handles MongoDB connection management
    """

    def __init__(self):
        self.client = None
        self.database = None
        self.uri = os.getenv(
            'MONGODB_URI',
            'mongodb+srv://calvarado:Andresito111@ivy.beuwz4f.mongodb.net/?retryWrites=true&w=majority&appName=ivy'
        )
        self.db_name = os.getenv('DB_NAME', 'trinity_market')

    async def connect(self):
        """Establish database connection"""
        try:
            self.client = AsyncIOMotorClient(self.uri)
            self.database = self.client[self.db_name]

            # Verify connection
            await self.client.admin.command('ping')
            logger.info(f"Connected to MongoDB database: {self.db_name}")
            return self.database

        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during database connection: {e}")
            raise

    async def disconnect(self):
        """Close database connection"""
        if self.client is not None:
            self.client.close()
            logger.info("Disconnected from MongoDB")

    def get_database(self):
        """Get database instance"""
        if self.database is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self.database

    def get_collection(self, collection_name: str):
        """Get collection from database"""
        if self.database is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self.database[collection_name]

class SecondaryDatabaseConfig:
    """
    Secondary Database configuration for trinity_performance_marketAnalysis
    This database is used to replicate market analysis data to Dev database
    """

    def __init__(self):
        self.client = None
        self.database = None
        self.uri = os.getenv(
            'SECONDARY_MONGODB_URI',
            'mongodb+srv://urieldev:urieldev@cluster0.yru42a6.mongodb.net/'
        )
        self.db_name = os.getenv('SECONDARY_DB_NAME', 'Dev')

    async def connect(self):
        """Establish secondary database connection"""
        try:
            self.client = AsyncIOMotorClient(self.uri)
            self.database = self.client[self.db_name]

            # Verify connection
            await self.client.admin.command('ping')
            logger.info(f"Connected to Secondary MongoDB database: {self.db_name}")
            return self.database

        except ConnectionFailure as e:
            logger.error(f"Failed to connect to Secondary MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during secondary database connection: {e}")
            raise

    async def disconnect(self):
        """Close secondary database connection"""
        if self.client is not None:
            self.client.close()
            logger.info("Disconnected from Secondary MongoDB")

    def get_database(self):
        """Get secondary database instance"""
        if self.database is None:
            raise RuntimeError("Secondary database not connected. Call connect() first.")
        return self.database

    def get_collection(self, collection_name: str):
        """Get collection from secondary database"""
        if self.database is None:
            raise RuntimeError("Secondary database not connected. Call connect() first.")
        return self.database[collection_name]

# Singleton instances
db_config = DatabaseConfig()
secondary_db_config = SecondaryDatabaseConfig()