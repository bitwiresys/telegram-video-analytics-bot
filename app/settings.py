from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str | None = None
    bot_token: str | None = None

    videos_json_path: str | None = None
    auto_import: bool = True

    openrouter_api_key: str | None = None
    openrouter_model: str | None = None
    openrouter_fallback_model: str | None = None
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    openrouter_timeout_seconds: float = 120.0


def get_settings() -> Settings:
    """Принимает ничего; возвращает Settings, загруженные из окружения/.env."""
    return Settings()
