from sqlalchemy import create_engine, text, insert
from sqlalchemy.engine import Engine
from loguru import logger
from typing import List, Dict, Any, Optional
from config import settings
import time

class DatabaseManager:
    """Handles PostgreSQL database connections, delta fetching, insertion and health logging."""

    def __init__(self, connection_url: str):
        self.engine: Engine = create_engine(
            connection_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True
        )
        self._init_health_table()

    def _init_health_table(self):
        """Ensures the health/audit table exists."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {settings.HEALTH_TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            process_name VARCHAR(255),
            status VARCHAR(50),
            records_processed INT,
            message TEXT
        );
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            logger.info(f"Ensured health table '{settings.HEALTH_TABLE_NAME}' exists.")
        except Exception as e:
            logger.error(f"Error initializing health table: {e}")

    def log_health(self, process_name: str, status: str, records_processed: int, message: str):
        """Logs execution status to the health table."""
        insert_sql = f"""
        INSERT INTO {settings.HEALTH_TABLE_NAME} (process_name, status, records_processed, message)
        VALUES (:pname, :status, :count, :msg)
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(insert_sql), {
                    "pname": process_name,
                    "status": status,
                    "count": records_processed,
                    "msg": message
                })
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to log health: {e}")

    def fetch_delta_data(self, table_name: str, delta_column: str, last_offset: Optional[str], limit: int, status_column: str, target_statuses: List[str], order_desc: bool = True) -> List[Dict[str, Any]]:
        """Fetches delta data with configurable status and ordering."""
        query_str = f"SELECT * FROM {table_name}"
        conditions = []
        params = {"limit": limit}

        if last_offset:
            op = "<" if order_desc else ">"
            conditions.append(f"{delta_column} {op} :last_offset")
            params["last_offset"] = last_offset

        if target_statuses:
            status_params = [f":status_{i}" for i in range(len(target_statuses))]
            conditions.append(f"{status_column} IN ({', '.join(status_params)})")
            for i, status in enumerate(target_statuses):
                params[f"status_{i}"] = status

        if conditions:
            query_str += " WHERE " + " AND ".join(conditions)
        
        order = "DESC" if order_desc else "ASC"
        query_str += f" ORDER BY {delta_column} {order} LIMIT :limit"
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query_str), params)
                return [dict(row._mapping) for row in result]
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise e

    def insert_records(self, table_name: str, records: List[Dict[str, Any]]):
        """Inserts records into the target table."""
        if not records:
            return
        
        try:
            with self.engine.connect() as conn:
                # Use a transaction for the batch insert
                with conn.begin():
                    # Batch insert using the table's column names
                    # Note: This assumes columns in records match target table
                    conn.execute(text(f"INSERT INTO {table_name} ({', '.join(records[0].keys())}) VALUES ({', '.join([':'+k for k in records[0].keys()])})"), records)
                logger.info(f"Successfully inserted {len(records)} records into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting records: {e}")
            raise e

def get_db_url() -> str:
    """Constructs the PostgreSQL connection URL."""
    return f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
