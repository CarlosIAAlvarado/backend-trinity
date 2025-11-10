import logging
from typing import Dict, Any, Optional
from datetime import datetime
from config.database import secondary_db_config
import asyncio

logger = logging.getLogger(__name__)

class SecondaryConfigRepository:
    """
    Repository for Config operations in SECONDARY database
    Handles CRUD operations for trinity_performance_config collection
    This is a replica/backup of the main config data
    Uses singleton pattern (only one config document)
    """

    def __init__(self):
        self.collection_name = 'trinity_performance_config'
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        self.config_type = 'app_config'  # Singleton identifier (same as primary DB)

    @property
    def collection(self):
        """Get the MongoDB collection from secondary database"""
        return secondary_db_config.get_collection(self.collection_name)

    async def upsert_config_with_retry(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert or update config with retry logic (Singleton pattern)

        Args:
            config_data: Dictionary containing config data

        Returns:
            Dictionary with operation result
        """
        for attempt in range(self.max_retries):
            try:
                result = await self._upsert_config(config_data)
                if attempt > 0:
                    logger.info(f"[SECONDARY DB] Successfully saved config after {attempt + 1} attempts")
                return result
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"[SECONDARY DB] Attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {self.retry_delay}s..."
                    )
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"[SECONDARY DB] All {self.max_retries} attempts failed. "
                        f"Config NOT saved to secondary database: {e}"
                    )
                    return {
                        'status': 'error',
                        'message': f'Failed after {self.max_retries} attempts: {str(e)}',
                        'action': 'failed'
                    }

    async def _upsert_config(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal method to upsert config (Singleton pattern)
        Uses EXACT same structure as PRIMARY database (snake_case)

        Args:
            config_data: Dictionary containing config data (snake_case fields)

        Returns:
            Dictionary with operation result
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)

            # Set timestamps (using snake_case like primary DB)
            current_time = datetime.now()
            config_data['last_updated'] = current_time

            # Ensure type field exists (same as primary DB)
            config_data['type'] = self.config_type

            # Check if config exists
            existing = await collection.find_one({'type': self.config_type})

            if not existing:
                action = "created"
            else:
                action = "updated"

            # Upsert config (replace/create singleton document)
            result = await collection.replace_one(
                {'type': self.config_type},
                config_data,
                upsert=True
            )

            market_cap = config_data.get('market_cap_filter', 'unknown')
            condition = config_data.get('filter_condition', 'unknown')

            logger.info(
                f"[SECONDARY DB] Config {action}: "
                f"market_cap={market_cap}, condition={condition} "
                f"-> trinity_performance_config"
            )

            return {
                'status': 'success',
                'action': action,
                'modified_count': result.modified_count if action == 'updated' else 0,
                'upserted_id': str(result.upserted_id) if result.upserted_id else None
            }

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error upserting config: {e}")
            raise

    async def get_config(self) -> Optional[Dict[str, Any]]:
        """
        Get the global config from secondary database

        Returns:
            Dictionary with config data or None if not found
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            config = await collection.find_one({'type': self.config_type})
            return config

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error getting config: {e}")
            return None

    async def count_configs(self) -> int:
        """
        Count config documents in secondary database (should always be 1)

        Returns:
            Number of config documents
        """
        try:
            collection = secondary_db_config.get_collection(self.collection_name)
            count = await collection.count_documents({})
            return count

        except Exception as e:
            logger.error(f"[SECONDARY DB] Error counting configs: {e}")
            return 0
