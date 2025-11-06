from pydantic import BaseModel

class App(BaseModel):
    host: str
    port: int
    
class NewRemotePeer(BaseModel):
    host: str
    port: int
    local_instances: list[App]
    
class FailoverInfo(BaseModel):
    responsible_peer: App
    failed_instance: App