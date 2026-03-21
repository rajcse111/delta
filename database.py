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

    def fetch_delta_data(self, table_name: str, delta_column: str, last_offset: Optional[str], limit: int = 2000, status_column: Optional[str] = None, target_statuses: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Fetches new or updated records from the database using a delta strategy with batching and filtering."""
        
        # Base query
        query_str = f"SELECT * FROM {table_name}"
        conditions = []
        params = {"limit": limit}

        # Add delta filter if an offset exists
        if last_offset:
            conditions.append(f"{delta_column} > :last_offset")
            params["last_offset"] = last_offset

        # Add status filter if configured
        if status_column and target_statuses:
            # target_statuses is a list, we need to format it for IN clause or pass as list if supported
            # SQLAlchemy text() with bindparams handles lists for IN clauses differently depending on driver,
            # but usually it's easier to expand manually or use specific syntax.
            # However, simpler: let's use a secure way.
            # We'll create parameters for each status.
            status_params = [f":status_{i}" for i in range(len(target_statuses))]
            conditions.append(f"{status_column} IN ({', '.join(status_params)})")
            for i, status in enumerate(target_statuses):
                params[f"status_{i}"] = status

        if conditions:
            query_str += " WHERE " + " AND ".join(conditions)
        
        # Order by delta column to ensure sequential processing
        query_str += f" ORDER BY {delta_column} ASC"
        
        # Add Limit for batching
        query_str += " LIMIT :limit"
        
        try:
            with self.engine.connect() as conn:
                logger.debug(f"Executing Query: {query_str} | Params: {params}")
                result = conn.execute(text(query_str), params)
                # Convert results to list of dicts
                rows = [dict(row._mapping) for row in result]
                logger.info(f"Fetched {len(rows)} records.")
                return rows
        except Exception as e:
            logger.error(f"Error fetching delta data: {e}")
            raise e # Raise to trigger retry mechanism

def get_db_url() -> str:
    """Constructs the MySQL connection URL."""
    return f"mysql+pymysql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
