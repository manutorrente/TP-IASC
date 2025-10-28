from pydantic import BaseModel

class App(BaseModel):
    host: str
    port: int