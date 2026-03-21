from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    api_url: str = "https://api.rocketil.live/api/alerts"
    poll_interval: int = 5  # seconds between API polls
    host: str = "0.0.0.0"
    port: int = 8000

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
