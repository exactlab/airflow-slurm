from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", case_sensitive=True, extra="ignore"
    )
    SSH_HOST: str
    SSH_PORT: int = 22
    SSH_USER: str
    SSH_PASSWORD: str | None = None 
    SSH_KEY_PATH: str | None = None 

settings = Settings()