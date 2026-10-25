from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, model_validator


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    database_url: str = "postgresql://shortener:shortener@localhost:5432/shortener"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "redirect-events"
    kafka_client_id: str = "url-shortener-api"
    code_length: int = Field(default=7, ge=3, le=32)
    cache_ttl_seconds: int = Field(default=3600, ge=1)
    event_publish_timeout_seconds: float = Field(default=1.0, gt=0)
    database_pool_min_size: int = Field(default=2, ge=1)
    database_pool_max_size: int = Field(default=8, ge=1)

    @model_validator(mode="after")
    def validate_pool_bounds(self) -> "Settings":
        if self.database_pool_min_size > self.database_pool_max_size:
            raise ValueError("database_pool_min_size cannot exceed database_pool_max_size")
        return self


settings = Settings()
