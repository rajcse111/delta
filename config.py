from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    DB_HOST: str = "localhost"
    DB_PORT: int = 5433
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "password"
    DB_NAME: str = "postgres"
    DB_TABLE_NAME: str = "users"
    FETCH_INTERVAL_MINUTES: int = 1
    DELTA_COLUMN: str = "updated_at"
    LAST_PROCESSED_OFFSET_FILE: str = "last_processed_offset.txt"
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
