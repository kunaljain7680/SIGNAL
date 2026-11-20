import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "SIGNAL"
    DATABASE_URL: str = "postgresql+asyncpg://signal_user:signal_password@localhost:5432/signal_db"

    # Automatically load from the .env file in the root directory (signal/.env)
    model_config = SettingsConfigDict(
        env_file=os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../.env")),
        env_file_encoding="utf-8", 
        extra="ignore"
    )

settings = Settings()