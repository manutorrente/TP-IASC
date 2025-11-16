from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    coordinator_url: str = "http://localhost:8000"
    node_host: str = "localhost"
    node_port: int = 8000
    write_quorum: int = 2
    modification_history_ttl_seconds: int = 3600
    
    class Config:
        env_file = ".env"

settings = Settings()