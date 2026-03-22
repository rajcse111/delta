import argparse
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

class DataProcessor:
    """Production-ready application for data transfer and status-based processing."""

    def __init__(self):
        self.db_manager = DatabaseManager(get_db_url())
        
        # State managers for different processes
        self.simple_migration_state = StateManager(
            use_db=settings.USE_DB_STATE_STORE,
            offset_file="simple_migration_offset.txt",
            db_engine=self.db_manager.engine,
            process_name="simple_data_transfer"
        )
        self.final_stage_state = StateManager(
            use_db=settings.USE_DB_STATE_STORE,
            offset_file="final_stage_offset.txt",
            db_engine=self.db_manager.engine,
            process_name="final_stage_sync"
        )
        self.intermediate_stage_state = StateManager(
            use_db=settings.USE_DB_STATE_STORE,
            offset_file="intermediate_stage_offset.txt",
            db_engine=self.db_manager.engine,
            process_name="intermediate_stage_sync"
        )

    def _execute_transfer(self, process_name: str, state_manager: StateManager, target_statuses: Optional[List[str]] = None, log_health: bool = False):
        """Core transfer logic used by both modes."""
        logger.info(f"Starting process: {process_name}")
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
                    status_column=settings.STATUS_COLUMN if target_statuses else None,
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
            logger.error(f"Process {process_name} failed: {e}")
            raise e
        finally:
            if log_health:
                self.db_manager.log_health(
                    process_name=process_name,
                    status=status,
                    records_processed=total_processed,
                    message=error_msg if status == "FAILED" else f"Successfully processed {total_processed} records."
                )

    def run_simple_migration(self):
        """Requirement 1: Data Transfer Logic (No status filter, no health logging)."""
        logger.info("Executing Simple Data Transfer Logic (Requirement #1)")
        self._execute_transfer(
            process_name="simple_migration",
            state_manager=self.simple_migration_state,
            target_statuses=None,
            log_health=False
        )

    def run_status_sync(self):
        """Requirement 2 & 3: Status-Based Dual-Stage Processing with Health Tracking."""
        logger.info("Executing Status-Based Dual-Stage Processing (Requirement #2 & #3)")
        
        # 1. Final Stage
        final_statuses = [s.strip() for s in settings.FINAL_STAGE_STATUSES.split(",") if s.strip()]
        self._execute_transfer(
            process_name="final_stage_sync",
            state_manager=self.final_stage_state,
            target_statuses=final_statuses,
            log_health=True
        )
        
        # 2. Intermediate Stage
        inter_statuses = [s.strip() for s in settings.INTERMEDIATE_STAGE_STATUSES.split(",") if s.strip()]
        self._execute_transfer(
            process_name="intermediate_stage_sync",
            state_manager=self.intermediate_stage_state,
            target_statuses=inter_statuses,
            log_health=True
        )

def main():
    parser = argparse.ArgumentParser(description="Data Migration and Status Sync CLI")
    parser.add_argument(
        "--mode", 
        choices=["transfer", "status-sync", "scheduler"], 
        required=True,
        help="Execution mode: 'transfer' for simple migration, 'status-sync' for status-based dual-stage, 'scheduler' to run periodically."
    )
    args = parser.parse_args()

    app = DataProcessor()

    if args.mode == "transfer":
        app.run_simple_migration()
    
    elif args.mode == "status-sync":
        app.run_status_sync()
    
    elif args.mode == "scheduler":
        logger.info(f"Starting scheduler mode. Daily at {settings.SCHEDULE_HOUR:02d}:{settings.SCHEDULE_MINUTE:02d}")
        scheduler = BlockingScheduler(timezone=settings.TIMEZONE)
        trigger = CronTrigger(hour=settings.SCHEDULE_HOUR, minute=settings.SCHEDULE_MINUTE)
        
        # You can choose which one to schedule, or both. Defaulting to both per previous setup.
        scheduler.add_job(app.run_simple_migration, trigger=trigger, id="simple_mig", name="Daily Transfer")
        scheduler.add_job(app.run_status_sync, trigger=trigger, id="status_sync", name="Daily Status Sync")
        
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Graceful shutdown.")
            scheduler.shutdown()

if __name__ == "__main__":
    main()
