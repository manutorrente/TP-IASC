from pydantic import BaseModel, Field
from typing import List
from .api import App


class AppConfig(BaseModel):
    host: str = Field(..., description="Sentinel host address")
    port: int = Field(..., description="Sentinel port number", gt=0, lt=65536)
    polling_interval: int = Field(..., description="Health check polling interval in seconds", gt=0)
    sentinel_peers: List[App] = Field(default_factory=list, description="Other sentinel peers in the cluster")
    app_instances: List[App] = Field(default_factory=list, description="Application instances to monitor")
