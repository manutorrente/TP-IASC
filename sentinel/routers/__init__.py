from .health import router as health_router
from .instances import router as instances_router
from .peers import router as peers_router
from .election import router as election_router
from .failover import router as failover_router

__all__ = [
    "health_router",
    "instances_router",
    "peers_router",
    "election_router",
    "failover_router",
]
