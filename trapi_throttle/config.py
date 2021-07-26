from pydantic import \
    BaseSettings, RedisDsn


class Settings(BaseSettings):
    redis_url: RedisDsn = "redis://localhost"

    class Config:
        env_file = ".env"


settings = Settings()
