from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    coordinator_url: str = "http://localhost:8000"
    node_host: str = "localhost"
    node_port: int = 8000
    write_quorum: int = 2
    modification_history_ttl_seconds: int = 3600
    
    # Sharding configuration
    enable_sharding: bool = True
    num_shards: int = 4
    shard_replication_factor: int = 3  # N/2+1 for 5 nodes
    
    class Config:
        env_file = ".env"

settings = Settings()