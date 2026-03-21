import os
from loguru import logger
from typing import Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine

class StateManager:
    """Manages the persistence of the last processed offset (e.g., timestamp or ID)."""

    def __init__(self, use_db: bool, offset_file: str, db_engine: Optional[Engine] = None, process_name: str = "default_process"):
        self.use_db = use_db
        self.offset_file = offset_file
        self.db_engine = db_engine
        self.process_name = process_name

        if self.use_db and self.db_engine:
            self._init_db_state_table()

    def _init_db_state_table(self):
        """Ensures the processing_state table exists."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS processing_state (
            process_name VARCHAR(255) PRIMARY KEY,
            last_processed_timestamp VARCHAR(255)
        );
        """
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            logger.info("Ensured processing_state table exists.")
        except Exception as e:
            logger.error(f"Error initializing state table: {e}")

    def get_last_offset(self) -> Optional[str]:
        """Reads the last processed offset from the configured store."""
        if self.use_db and self.db_engine:
            return self._get_offset_from_db()
        else:
            return self._get_offset_from_file()

    def save_last_offset(self, offset: str):
        """Saves the last processed offset to the configured store."""
        if self.use_db and self.db_engine:
            self._save_offset_to_db(offset)
        else:
            self._save_offset_to_file(offset)

    def _get_offset_from_file(self) -> Optional[str]:
        if not os.path.exists(self.offset_file):
            logger.info(f"Offset file {self.offset_file} not found. Starting from scratch.")
            return None
        
        try:
            with open(self.offset_file, "r") as f:
                offset = f.read().strip()
                if not offset:
                    return None
                logger.debug(f"Last processed offset retrieved from file: {offset}")
                return offset
        except Exception as e:
            logger.error(f"Error reading offset file: {e}")
            return None

    def _save_offset_to_file(self, offset: str):
        try:
            with open(self.offset_file, "w") as f:
                f.write(str(offset))
            logger.debug(f"Saved last processed offset to file: {offset}")
        except Exception as e:
            logger.error(f"Error saving offset file: {e}")

    def _get_offset_from_db(self) -> Optional[str]:
        query = text("SELECT last_processed_timestamp FROM processing_state WHERE process_name = :pname")
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(query, {"pname": self.process_name}).fetchone()
                if result:
                    offset = result[0]
                    logger.debug(f"Last processed offset retrieved from DB: {offset}")
                    return offset
                return None
        except Exception as e:
            logger.error(f"Error reading offset from DB: {e}")
            return None

    def _save_offset_to_db(self, offset: str):
        # UPSERT (Insert or Update) logic depends on DB type.
        # Since we use SQLAlchemy, we can try a generic approach or standard SQL.
        # MySQL supports "ON DUPLICATE KEY UPDATE".
        # PostgreSQL supports "ON CONFLICT".
        # For simplicity and broad compatibility without complex dialect checking here (assuming MySQL per request):
        
        # We'll use a simple check-then-update or insert strategy for broad compatibility if we don't want dialect specific SQL here,
        # OR use the specific syntax since we know it's MySQL now.
        # Given the previous context was MySQL, let's use MySQL syntax but keep it somewhat clean.
        
        # Actually, standard SQL MERGE is not supported everywhere.
        # Let's try a delete-insert or update-if-exists.
        
        # MySQL specific:
        upsert_sql = """
        INSERT INTO processing_state (process_name, last_processed_timestamp)
        VALUES (:pname, :offset)
        ON DUPLICATE KEY UPDATE last_processed_timestamp = :offset
        """
        
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text(upsert_sql), {"pname": self.process_name, "offset": str(offset)})
                conn.commit()
            logger.debug(f"Saved last processed offset to DB: {offset}")
        except Exception as e:
            logger.error(f"Error saving offset to DB: {e}")
