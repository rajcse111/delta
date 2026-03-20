from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from loguru import logger
from typing import List, Dict, Any, Optional
from config import settings
import time

class DatabaseManager:
    """Handles PostgreSQL database connections and delta fetching logic."""

    def __init__(self, connection_url: str):
        self.engine: Engine = create_engine(
            connection_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True
        )

    def fetch_delta_data(self, table_name: str, delta_column: str, last_offset: Optional[str]) -> List[Dict[str, Any]]:
        """Fetches new or updated records from the database using a delta strategy."""
        
        # Base query
        query_str = f"SELECT * FROM {table_name}"
        
        # Add delta filter if an offset exists
        if last_offset:
            query_str += f" WHERE {delta_column} > :last_offset"
        
        # Order by delta column to ensure sequential processing
        query_str += f" ORDER BY {delta_column} ASC"
        
        try:
            with self.engine.connect() as conn:
                logger.info(f"Fetching data from {table_name} where {delta_column} > {last_offset or 'START'}")
                result = conn.execute(text(query_str), {"last_offset": last_offset})
                # Convert results to list of dicts
                rows = [dict(row._mapping) for row in result]
                logger.info(f"Fetched {len(rows)} new records.")
                return rows
        except Exception as e:
            logger.error(f"Error fetching delta data: {e}")
            raise e # Raise to trigger retry mechanism

def get_db_url() -> str:
    """Constructs the database connection URL."""
    return f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
