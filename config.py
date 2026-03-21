from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    DB_HOST: str = "localhost"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: str = ""
    DB_NAME: str = "test"
    DB_TABLE_NAME: str = "users"
    FETCH_INTERVAL_MINUTES: int = 1  # Kept for backward compatibility, but we will use CRON mostly
    SCHEDULE_HOUR: int = 20
    SCHEDULE_MINUTE: int = 0
    TIMEZONE: str = "UTC"
    
    BATCH_SIZE: int = 2000
    STATUS_COLUMN: Optional[str] = None # e.g., "status"
    TARGET_STATUSES: Optional[str] = None # e.g., "COMPLETED,FAILED" (comma separated)

    DELTA_COLUMN: str = "updated_at"
    LAST_PROCESSED_OFFSET_FILE: str = "last_processed_offset.txt"
    USE_DB_STATE_STORE: bool = True
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
