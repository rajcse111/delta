from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    DB_HOST: str = "localhost"
    DB_PORT: int = 5433
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "password"
    DB_NAME: str = "postgres"
    # Multi-table Configuration
    # Format: "source_table:target_table:process_id,source_table2:target_table2:process_id2"
    MIGRATION_TABLES: str = "users:test_users:user_migration"
    
    HEALTH_TABLE_NAME: str = "health"
    PRIMARY_KEY_COLUMN: str = "id"
    
    FETCH_INTERVAL_MINUTES: int = 1
    SCHEDULE_HOUR: int = 20
    SCHEDULE_MINUTE: int = 0
    TIMEZONE: str = "UTC"
    
    BATCH_SIZE: int = 2000
    STATUS_COLUMN: str = "status"
    FINAL_STAGE_STATUSES: str = "COMPLETED,FAILED,CLOSED"
    INTERMEDIATE_STAGE_STATUSES: str = "PENDING,PROCESSING,IN_PROGRESS"

    DELTA_COLUMN: str = "registration_date"
    ORDER_BY_DESC: bool = True # Per CR.pdf requirement for Descending order
    LAST_PROCESSED_OFFSET_FILE: str = "last_processed_offset.txt"
    USE_DB_STATE_STORE: bool = True
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
