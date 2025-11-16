from pydantic import BaseModel

class App(BaseModel):
    host: str
    port: int
    
    def __hash__(self) -> int:
        return hash((self.host, self.port))
    
class APIAddressModel(BaseModel):
    address: App
    
class CoordinatorUpdate(BaseModel):
    coordinator_pick: APIAddressModel
    origin: APIAddressModel
    
class NewRemotePeer(BaseModel):
    host: str
    port: int
    local_instances: list[App]
    
class FailoverInfo(BaseModel):
    responsible_peer: App
    failed_instance: App