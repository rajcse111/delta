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

class DataMigrationApp:
    """Production-ready application for data transfer and health tracking."""

    def __init__(self):
        self.db_manager = DatabaseManager(get_db_url())
        
        # State managers for different stages
        self.final_stage_state = StateManager(
            use_db=settings.USE_DB_STATE_STORE,
            offset_file="final_stage_offset.txt",
            db_engine=self.db_manager.engine,
            process_name="final_stage_migration"
        )
        self.intermediate_stage_state = StateManager(
            use_db=settings.USE_DB_STATE_STORE,
            offset_file="intermediate_stage_offset.txt",
            db_engine=self.db_manager.engine,
            process_name="intermediate_stage_migration"
        )

    def run_migration(self, process_name: str, state_manager: StateManager, target_statuses: List[str]):
        """Runs the migration for a specific set of statuses."""
        logger.info(f"Starting migration: {process_name}")
        total_processed = 0
        status = "SUCCESS"
        error_msg = ""

        try:
            while True:
                last_offset = state_manager.get_last_offset()
                
                # Fetch records
                records = self.db_manager.fetch_delta_data(
                    table_name=settings.DB_TABLE_NAME,
                    delta_column=settings.DELTA_COLUMN,
                    last_offset=last_offset,
                    limit=settings.BATCH_SIZE,
                    status_column=settings.STATUS_COLUMN,
                    target_statuses=target_statuses,
                    order_desc=settings.ORDER_BY_DESC
                )

                if not records:
                    logger.info(f"No more records for {process_name}")
                    break

                # Transfer data
                self.db_manager.insert_records(settings.TARGET_TABLE_NAME, records)
                
                # Update state
                new_offset = str(records[-1].get(settings.DELTA_COLUMN))
                state_manager.save_last_offset(new_offset)
                
                total_processed += len(records)
                
                if len(records) < settings.BATCH_SIZE:
                    break

        except Exception as e:
            status = "FAILED"
            error_msg = str(e)
            logger.error(f"Migration {process_name} failed: {e}")
        finally:
            self.db_manager.log_health(
                process_name=process_name,
                status=status,
                records_processed=total_processed,
                message=error_msg if status == "FAILED" else f"Successfully migrated {total_processed} records."
            )

    def run_all_tasks(self):
        """Executes migration for both final and intermediate stages."""
        logger.info("Executing scheduled data migration pipeline.")
        
        # Final Stage
        final_statuses = [s.strip() for s in settings.FINAL_STAGE_STATUSES.split(",") if s.strip()]
        self.run_migration("final_stage", self.final_stage_state, final_statuses)
        
        # Intermediate Stage
        inter_statuses = [s.strip() for s in settings.INTERMEDIATE_STAGE_STATUSES.split(",") if s.strip()]
        self.run_migration("intermediate_stage", self.intermediate_stage_state, inter_statuses)

def main():
    logger.info("Starting Data Migration System...")
    app = DataMigrationApp()
    
    scheduler = BlockingScheduler(timezone=settings.TIMEZONE)
    trigger = CronTrigger(hour=settings.SCHEDULE_HOUR, minute=settings.SCHEDULE_MINUTE)
    
    scheduler.add_job(
        app.run_all_tasks,
        trigger=trigger,
        id="data_migration_job",
        name="Scheduled Data Transfer"
    )
    
    logger.info(f"Scheduled at {settings.SCHEDULE_HOUR:02d}:{settings.SCHEDULE_MINUTE:02d} {settings.TIMEZONE}")
    
    try:
        # app.run_all_tasks() # Run once on start for validation
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Graceful shutdown.")
        scheduler.shutdown()

if __name__ == "__main__":
    main()
