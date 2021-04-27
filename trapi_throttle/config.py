from datetime import timedelta
from pydantic import \
    BaseSettings, FilePath, RedisDsn, AnyUrl


class Settings(BaseSettings):
    redis_url: RedisDsn = "redis://redis"

    class Config:
        env_file = ".env"


settings = Settings()
