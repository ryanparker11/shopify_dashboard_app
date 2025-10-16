# commerce_app/config/settings.py
from pydantic import BaseSettings

class Settings(BaseSettings):
    shopify_api_key: str
    shopify_secret: str
    postgres_url: str | None = None

    class Config:
        env_file = ".env"

settings = Settings()
