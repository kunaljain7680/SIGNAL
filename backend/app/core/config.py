from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    PROJECT_NAME: str = "SIGNAL"
    DATABASE_URL: str = "postgresql+asyncpg://signal_user:signal_password@localhost:5432/signal_db"
    SIGNAL_GITHUB_PAT: str | None = None

    # Load the repository-root .env without copying values into source control.
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
