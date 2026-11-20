from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/urlshortener"
    base_url: str = "http://localhost:8000"

    class Config:
        env_file = ".env"


settings = Settings()