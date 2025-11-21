from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
import sys
import uvicorn
import logging
import yaml


from models.domain import Cluster
from models.config import AppConfig
from models.api import App
from services.peer_service import PeerService, RemoteSentinelPeer
from services.polling_service import start_background_tasks
from utils import async_request
from routers import (
    health_router,
    instances_router,
    peers_router,
    election_router,
    failover_router
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config(path: str) -> AppConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    return AppConfig(**raw)


async def discover_peer_outgoing(host: str, port: int, app: FastAPI) -> list[App]:
    url = f"http://{host}:{port}/discover-peer"
    payload = {
        "host": app.state.config.host,
        "port": app.state.config.port,
        "local_instances": [app.model_dump() for app in app.state.cluster.get_instances_list()]
    }
    try:
        response = await async_request("POST", url, json=payload)
        new_instances_data = response.get("local_instances", [])
        new_instances = [App(**data) for data in new_instances_data]
        logger.info(f"Discovered {len(new_instances)} new instances from peer {host}:{port}")
        return new_instances
    except Exception as e:
        logger.info(f"Error discovering peer {host}:{port} - {e}. Peer is possibly down")
        return []


@asynccontextmanager
async def lifespan(app: FastAPI):
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    config = load_config(config_path)

    app.state.config = config
    app.state.cluster = Cluster(instances=[])
    app.state.peer_service = PeerService(self_peer=RemoteSentinelPeer(config.host, config.port), cluster=app.state.cluster)
    app.state.failover_in_progress = False

    for instance in config.app_instances:
        app.state.cluster.add_instance(instance.host, instance.port)

    for peer_sentinel in config.sentinel_peers:
        peer = RemoteSentinelPeer(peer_sentinel.host, peer_sentinel.port)
        try:
            new_instances = await discover_peer_outgoing(peer.host, peer.port, app)
            app.state.cluster.add_instances(new_instances)

        except Exception as e:
            logger.error(f"Error discovering peer {peer.host}:{peer.port} - {e}. Peer is possibly down", exc_info=True)
        app.state.peer_service.add_peer(peer)

    start_background_tasks(app)

    yield


app = FastAPI(title="Sentinel API", lifespan=lifespan)

app.include_router(health_router)
app.include_router(instances_router)
app.include_router(peers_router)
app.include_router(election_router)
app.include_router(failover_router)


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "sentinel/config.yaml"
    config = load_config(config_file)
    uvicorn.run(app, host="0.0.0.0", port=config.port)
