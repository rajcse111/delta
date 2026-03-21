from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
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
        
        # Initialize StateManager with DB engine if enabled
        self.state_manager = StateManager(
            use_db=settings.USE_DB_STATE_STORE,
            offset_file=settings.LAST_PROCESSED_OFFSET_FILE,
            db_engine=self.db_manager.engine if settings.USE_DB_STATE_STORE else None,
            process_name="user_data_sync" # Unique identifier for this process
        )
        
        self.table_to_process = settings.DB_TABLE_NAME

    def process_records(self, records: List[Dict[str, Any]]) -> Optional[Any]:
        """Business logic for processing the fetched records."""
        if not records:
            return None
        
        # In a real scenario, process records here (e.g., push to Kafka, write to another DB, etc.)
        for record in records:
            # logger.debug(f"Processing record: {record}") # Commented out to reduce noise in large batches
            pass
            
        logger.info(f"Processed batch of {len(records)} records.")
        
        # Returns the last offset to be saved
        return records[-1].get(settings.DELTA_COLUMN)

    def run_task(self):
        """Runs the incremental fetching and processing task with batching and retries."""
        
        logger.info("Starting scheduled task execution...")
        
        # Parse target statuses
        target_statuses = None
        if settings.STATUS_COLUMN and settings.TARGET_STATUSES:
            target_statuses = [s.strip() for s in settings.TARGET_STATUSES.split(",") if s.strip()]

        batch_size = settings.BATCH_SIZE
        total_processed = 0
        
        while True:
            # Batch Loop
            try:
                # 1. Get last processed offset
                last_offset = self.state_manager.get_last_offset()
                
                # 2. Fetch delta data (Batch)
                records = self.db_manager.fetch_delta_data(
                    table_name=self.table_to_process,
                    delta_column=settings.DELTA_COLUMN,
                    last_offset=last_offset,
                    limit=batch_size,
                    status_column=settings.STATUS_COLUMN,
                    target_statuses=target_statuses
                )
                
                if not records:
                    logger.info("No more records to process in this run.")
                    break
                
                # 3. Process records
                new_offset = self.process_records(records)
                
                # 4. Save the new offset (Idempotency checkpoint)
                if new_offset:
                    self.state_manager.save_last_offset(new_offset)
                    logger.debug(f"Checkpoint saved: {new_offset}")
                
                total_processed += len(records)
                
                # If we fetched fewer records than the batch size, we are done
                if len(records) < batch_size:
                    logger.info("Reached end of available data.")
                    break
                    
            except Exception as e:
                logger.error(f"Error during batch execution: {e}")
                # Optional: Add retry logic here specifically for the batch or break to retry next schedule
                # For now, we will break to avoid infinite error loops
                break
        
        logger.info(f"Task execution completed. Total records processed: {total_processed}")

def main():
    """Application entry point."""
    logger.info("Starting Delta Fetching Application...")
    
    app = DeltaApp()
    
    # Initialize scheduler
    scheduler = BlockingScheduler(timezone=settings.TIMEZONE)
    
    # Schedule the task using CronTrigger
    trigger = CronTrigger(
        hour=settings.SCHEDULE_HOUR,
        minute=settings.SCHEDULE_MINUTE
    )
    
    scheduler.add_job(
        app.run_task,
        trigger=trigger,
        id="delta_fetch_job",
        name="Daily Delta Fetch"
    )
    
    logger.info(f"Task scheduled to run daily at {settings.SCHEDULE_HOUR:02d}:{settings.SCHEDULE_MINUTE:02d} {settings.TIMEZONE}.")
    
    try:
        # Run the first task immediately for testing/validation (Optional, can be removed for pure production)
        # app.run_task() 
        
        # Start the scheduler
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down the application gracefully.")
        scheduler.shutdown()

if __name__ == "__main__":
    main()
