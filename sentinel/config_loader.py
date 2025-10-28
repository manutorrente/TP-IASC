import yaml
from typing import List
from pydantic import BaseModel
from .models import App

class AppConfig(BaseModel):
    port: int
    polling_interval: int
    sentinel_peers: List[App]
    app_instances: List[App]

def load_config(path: str) -> AppConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    return AppConfig(**raw)
