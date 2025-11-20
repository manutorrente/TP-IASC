from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class App(BaseModel):
    host: str = Field(..., description="Application host address")
    port: int = Field(..., description="Application port number", gt=0, lt=65536)

    def __hash__(self) -> int:
        return hash((self.host, self.port))


class InstanceNotification(BaseModel):
    host: str = Field(..., description="Instance host address")
    port: int = Field(..., description="Instance port number", gt=0, lt=65536)


class PeerDiscoveryRequest(BaseModel):
    host: str = Field(..., description="Requesting peer host address")
    port: int = Field(..., description="Requesting peer port number", gt=0, lt=65536)
    local_instances: List[App] = Field(default_factory=list, description="Instances monitored by requesting peer")


class PeerDiscoveryResponse(BaseModel):
    message: str = Field(..., description="Response message")
    local_instances: List[App] = Field(..., description="Local instances monitored by this sentinel")


class CoordinatorUpdate(BaseModel):
    coordinator_pick: "CoordinatorAddress" = Field(..., description="Chosen coordinator")
    origin: "CoordinatorAddress" = Field(..., description="Peer that sent the update")


class CoordinatorAddress(BaseModel):
    address: App = Field(..., description="Coordinator address")


class CoordinatorChange(BaseModel):
    address: App = Field(..., description="New coordinator address")


class FailoverNotification(BaseModel):
    responsible_peer: App = Field(..., description="Peer responsible for failover")
    failed_instance: App = Field(..., description="Instance that failed")


class MasterNotification(BaseModel):
    address: App = Field(..., description="New master instance address")


class InstanceHealth(BaseModel):
    is_locally_up: bool = Field(..., description="Whether instance is up according to local checks")
    remote_down_counter: int = Field(..., description="Number of remote peers reporting this instance as down")


class ClusterStatusResponse(BaseModel):
    total_instances: int = Field(..., description="Total number of instances in cluster")
    health_status: Dict[str, InstanceHealth] = Field(..., description="Health status of each instance")
    master: Optional[str] = Field(None, description="Current master instance address")


class PeersResponse(BaseModel):
    total_peers: int = Field(..., description="Total number of sentinel peers")
    peers: List[str] = Field(..., description="List of peer addresses")
    offline_peers: List[str] = Field(..., description="List of offline peer addresses")
    coordinator: Optional[str] = Field(None, description="Current coordinator address")


class HealthResponse(BaseModel):
    status: str = Field("ok", description="Health status")
    message: str = Field("Service is healthy", description="Health message")
