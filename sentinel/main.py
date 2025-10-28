from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import asyncio
import sys
import uvicorn
import logging

from .instances import Cluster, SentinelPeer, AppInstance
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
    app.state.peers = []
    
    for instance in config.app_instances:
        app.state.cluster.add_instance(instance.host, instance.port)
    
    for peer_sentinel in config.sentinel_peers:
        peer = SentinelPeer(peer_sentinel.host, peer_sentinel.port)
        app.state.peers.append(peer)
        
        new_instances = await peer.discover_peer(
            local_host="localhost",
            local_port=config.port,
            cluster=app.state.cluster
        )
        app.state.cluster.add_instances(new_instances)

    start_background_task(app)
    
    yield

app = FastAPI(title="Health Check API", lifespan=lifespan)


# ===== DEPENDENCY FUNCTIONS =====

def get_cluster(request: Request) -> Cluster:
    """Provides access to the shared cluster instance"""
    return request.app.state.cluster


def get_peers(request: Request) -> list[SentinelPeer]:
    """Provides access to the list of sentinel peers"""
    return request.app.state.peers


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
    peers: list[SentinelPeer] = Depends(get_peers),
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
    
    if not any(p.host == host and p.port == port for p in peers):
        peer = SentinelPeer(host, port)
        peers.append(peer)
        logger.info(f"New peer discovered: {host}:{port}")
    
    for instance in new_instances:
        ihost = instance.get("host")
        iport = instance.get("port")
        if ihost and iport:
            if not cluster.get_instance(ihost, iport):
                cluster.add_instance(ihost, iport)
                logger.info(f"Added new instance from peer: {ihost}:{iport}")
    
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
async def list_peers(peers: list[SentinelPeer] = Depends(get_peers)):
    """List all known sentinel peers"""
    return {
        "total_peers": len(peers),
        "peers": [{"host": p.host, "port": p.port} for p in peers]
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