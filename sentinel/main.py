from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import asyncio
import sys
import uvicorn
import logging

from .instances import Cluster, SentinelCluster, AppInstance
from .config_loader import load_config, AppConfig as Config
from .polling_task import start_background_task

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    config = load_config(config_path)
    
    app.state.config = config
    app.state.cluster = Cluster(instances=[])
    app.state.sentinel_cluster = SentinelCluster()
    
    for instance in config.app_instances:
        app.state.cluster.add_instance(instance.host, instance.port)
    
    for peer_sentinel in config.sentinel_peers:
        peer = app.state.sentinel_cluster.add_peer(peer_sentinel.host, peer_sentinel.port)

    new_instances = await app.state.sentinel_cluster.discover_all_peers("localhost", config.port, app.state.cluster)
    app.state.cluster.add_instances(new_instances)

    start_background_task(app)
    
    yield

app = FastAPI(title="Health Check API", lifespan=lifespan)


# ===== DEPENDENCY FUNCTIONS =====

def get_cluster(request: Request) -> Cluster:
    """Provides access to the shared cluster instance"""
    return request.app.state.cluster


def get_sentinels(request: Request) -> SentinelCluster:
    """Provides access to the list of sentinel peers"""
    return request.app.state.sentinel_cluster


def get_config(request: Request) -> Config:
    """Provides access to the application configuration"""
    return request.app.state.config


# ===== ENDPOINTS =====

@app.post("/instance-down")
async def instance_down_notification(
    request: Request,
    cluster: Cluster = Depends(get_cluster)
):
    """
    Receive notification that an instance is down from a peer sentinel.
    """
    data = await request.json()
    host = data.get("host")
    port = data.get("port")
    
    if not host or not port:
        return JSONResponse(
            content={"error": "Missing host or port"},
            status_code=400
        )
    
    # Find the instance and mark it as down
    instance = cluster.get_instance(host, port)
    if instance:
        instance.down_counter += 1
        logger.info(f"Received notification: {host}:{port} is down (count: {instance.down_counter})")
        return JSONResponse(
            content={"message": "Instance marked as down"},
            status_code=200
        )
    
    return JSONResponse(
        content={"error": "Instance not found"},
        status_code=404
    )


@app.post("/discover-peer")
async def discover_peer(
    request: Request,
    cluster: Cluster = Depends(get_cluster),
    sentinel_cluster: SentinelCluster = Depends(get_sentinels),
    config: Config = Depends(get_config)
):
    """
    Handle peer discovery requests.
    Exchange cluster information with the requesting peer.
    """
    data = await request.json()
    host = data.get("host")
    port = data.get("port")
    new_instances = data.get("instances", [])
    
    if not host or not port:
        return JSONResponse(
            content={"error": "Invalid data: missing host or port"},
            status_code=400
        )
    
    sentinel_cluster.add_peer(host, port)
    
    cluster.add_instances(new_instances)
    
    local_cluster_info = cluster.get_instances_list()
    
    return JSONResponse(
        content={
            "message": "Peer discovered",
            "local_instances": [app.model_dump() for app in local_cluster_info]
        },
        status_code=200
    )


@app.get("/cluster-status")
async def cluster_status(cluster: Cluster = Depends(get_cluster)):
    """Get the current status of all instances in the cluster"""
    health_results = await cluster.check_health()
    return {
        "total_instances": len(cluster.instances),
        "health_status": health_results
    }


@app.get("/peers")
async def list_peers(sentinel_cluster: SentinelCluster = Depends(get_sentinels)):
    """List all known sentinel peers"""
    return {
        "total_peers": len(sentinel_cluster.peers),
        "peers": [{"host": p.host, "port": p.port} for p in sentinel_cluster.peers]
    }


@app.get("/health", tags=["Health"])
async def health_check():
    await asyncio.sleep(0.1)
    return JSONResponse(
        content={"status": "ok", "message": "Service is healthy"}
    )


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "sentinel/config.yaml"
    config = load_config(config_file)
    uvicorn.run(app, host="0.0.0.0", port=config.port)