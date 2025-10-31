from pydantic import BaseModel

class App(BaseModel):
    host: str
    port: int
    
class NewRemotePeer(BaseModel):
    host: str
    port: int
    local_instances: list[App] = []
    
