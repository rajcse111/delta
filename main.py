from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
import sys
import time
from config import settings
from database import DatabaseManager, get_db_url
from state import StateManager
from typing import List, Dict, Any, Optional

# Configure Loguru
logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>", level=settings.LOG_LEVEL)

class DeltaApp:
    """The main application for incremental data fetching and processing."""

    def __init__(self):
        self.db_manager = DatabaseManager(get_db_url())
        self.state_manager = StateManager(settings.LAST_PROCESSED_OFFSET_FILE)
        self.table_to_process = settings.DB_TABLE_NAME

    def process_records(self, records: List[Dict[str, Any]]) -> Optional[Any]:
        """Business logic for processing the fetched records."""
        if not records:
            return None
        
        # In a real scenario, process records here (e.g., push to Kafka, write to another DB, etc.)
        for record in records:
            logger.debug(f"Processing record: {record}")
        
        # Returns the last offset to be saved
        return records[-1].get(settings.DELTA_COLUMN)

    def run_task(self):
        """Runs the incremental fetching and processing task with retries."""
        
        max_retries = 3
        retry_delay = 5 # seconds
        
        for attempt in range(1, max_retries + 1):
            try:
                # 1. Get last processed offset
                last_offset = self.state_manager.get_last_offset()
                
                # 2. Fetch delta data
                records = self.db_manager.fetch_delta_data(
                    table_name=self.table_to_process,
                    delta_column=settings.DELTA_COLUMN,
                    last_offset=last_offset
                )
                
                # 3. Process records and get the new offset
                if records:
                    new_offset = self.process_records(records)
                    
                    # 4. Save the new offset (Idempotency checkpoint)
                    if new_offset:
                        self.state_manager.save_last_offset(new_offset)
                        logger.info(f"Task completed successfully. Last offset updated to: {new_offset}")
                else:
                    logger.info("No new records to process.")
                
                # If successful, break the retry loop
                break
                
            except Exception as e:
                logger.error(f"Error during execution (Attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2 # Exponential backoff
                else:
                    logger.critical("Max retries reached. Task failed.")

def main():
    """Application entry point."""
    logger.info("Starting Delta Fetching Application...")
    
    app = DeltaApp()
    
    # Initialize scheduler
    scheduler = BlockingScheduler()
    
    # Schedule the task
    scheduler.add_job(
        app.run_task,
        "interval",
        minutes=settings.FETCH_INTERVAL_MINUTES,
        id="delta_fetch_job"
    )
    
    logger.info(f"Task scheduled every {settings.FETCH_INTERVAL_MINUTES} minute(s).")
    
    try:
        # Run the first task immediately
        app.run_task()
        
        # Start the scheduler
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down the application gracefully.")
        scheduler.shutdown()

if __name__ == "__main__":
    main()
