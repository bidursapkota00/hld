from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str = "postgresql://user:pass@localhost:5432/urlshortener"
    base_url: str = "http://localhost:8000"
    short_code_length: int = 7
    default_ttl_days: int = 365
    machine_id: int = 0


settings = Settings()