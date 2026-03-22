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
    """Production-ready application for large-scale data transfer with multi-table support."""

    def __init__(self):
        self.db_manager = DatabaseManager(get_db_url())

    def _get_state_manager(self, process_name: str, suffix: str = "") -> StateManager:
        """Helper to create a StateManager for a specific process/table."""
        return StateManager(
            use_db=settings.USE_DB_STATE_STORE,
            offset_file=f"{process_name}{suffix}_offset.txt",
            db_engine=self.db_manager.engine,
            process_name=f"{process_name}{suffix}"
        )

    def _execute_transfer(self, source_table: str, target_table: str, process_name: str, target_statuses: Optional[List[str]] = None, log_health: bool = False):
        """Core transfer logic optimized for millions of records with batching and state persistence."""
        logger.info(f"Starting transfer from {source_table} to {target_table} (Process: {process_name})")
        
        state_manager = self._get_state_manager(process_name)
        total_processed = 0
        status = "SUCCESS"
        error_msg = ""

        try:
            while True:
                # 1. Get last processed offset (Supports resuming from failure)
                last_offset = state_manager.get_last_offset()
                
                # 2. Fetch delta records in batches
                records = self.db_manager.fetch_delta_data(
                    table_name=source_table,
                    delta_column=settings.DELTA_COLUMN,
                    last_offset=last_offset,
                    limit=settings.BATCH_SIZE,
                    status_column=settings.STATUS_COLUMN if target_statuses else None,
                    target_statuses=target_statuses,
                    order_desc=settings.ORDER_BY_DESC
                )

                if not records:
                    logger.info(f"No more records found for {process_name}. Migration complete.")
                    break

                # 3. Insert records into target table (Preserving IDs)
                self.db_manager.insert_records(
                    table_name=target_table, 
                    records=records,
                    primary_key=settings.PRIMARY_KEY_COLUMN
                )
                
                # 4. Save checkpoint (State persistence for millions of records)
                new_offset = str(records[-1].get(settings.DELTA_COLUMN))
                state_manager.save_last_offset(new_offset)
                
                total_processed += len(records)
                logger.info(f"Progress [{process_name}]: {total_processed} records migrated...")
                
                # Exit loop if we've reached the end of the data
                if len(records) < settings.BATCH_SIZE:
                    logger.info(f"Finished processing all available records for {process_name}.")
                    break

        except Exception as e:
            status = "FAILED"
            error_msg = str(e)
            logger.error(f"Migration {process_name} failed at {total_processed} records: {e}")
            raise e
        finally:
            if log_health:
                self.db_manager.log_health(
                    process_name=process_name,
                    status=status,
                    records_processed=total_processed,
                    message=error_msg if status == "FAILED" else f"Successfully migrated {total_processed} records."
                )

    def run_multi_table_migration(self, mode: str):
        """Iterates through configured tables and runs migration based on mode."""
        
        # Parse configuration: "source:target:pid,source2:target2:pid2"
        table_configs = [t.strip() for t in settings.MIGRATION_TABLES.split(",") if t.strip()]
        
        for config in table_configs:
            parts = config.split(":")
            if len(parts) != 3:
                logger.error(f"Invalid table config format: {config}. Expected source:target:process_id")
                continue
            
            source, target, pid = parts
            
            if mode == "transfer":
                self._execute_transfer(source, target, f"{pid}_simple", target_statuses=None, log_health=False)
            
            elif mode == "status-sync":
                # Final Stage
                final_statuses = [s.strip() for s in settings.FINAL_STAGE_STATUSES.split(",") if s.strip()]
                self._execute_transfer(source, target, f"{pid}_final", target_statuses=final_statuses, log_health=True)
                
                # Intermediate Stage
                inter_statuses = [s.strip() for s in settings.INTERMEDIATE_STAGE_STATUSES.split(",") if s.strip()]
                self._execute_transfer(source, target, f"{pid}_inter", target_statuses=inter_statuses, log_health=True)

def main():
    parser = argparse.ArgumentParser(description="Multi-Table Large-Scale Migration CLI")
    parser.add_argument(
        "--mode", 
        choices=["transfer", "status-sync", "scheduler"], 
        required=True,
        help="Execution mode."
    )
    args = parser.parse_args()

    app = DataProcessor()

    if args.mode in ["transfer", "status-sync"]:
        app.run_multi_table_migration(args.mode)
    
    elif args.mode == "scheduler":
        logger.info(f"Starting scheduler. Daily at {settings.SCHEDULE_HOUR:02d}:{settings.SCHEDULE_MINUTE:02d}")
        scheduler = BlockingScheduler(timezone=settings.TIMEZONE)
        trigger = CronTrigger(hour=settings.SCHEDULE_HOUR, minute=settings.SCHEDULE_MINUTE)
        
        scheduler.add_job(lambda: app.run_multi_table_migration("transfer"), trigger=trigger, id="multi_transfer")
        scheduler.add_job(lambda: app.run_multi_table_migration("status-sync"), trigger=trigger, id="multi_status")
        
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Graceful shutdown.")
            scheduler.shutdown()

if __name__ == "__main__":
    main()
