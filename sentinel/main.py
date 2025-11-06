from urllib import request
from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import asyncio
import sys
import uvicorn
import logging

from instances import Cluster, AppInstance
from remote_peers import RemoteSentinelPeer, RemotePeers
from config_loader import load_config, AppConfig as Config
from polling_task import start_background_task
from utils import async_request
from models import App, FailoverInfo, NewRemotePeer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def discover_peer_outgoing(host: str, port: int, app: FastAPI) -> list[App]:
    """Discover new instances from a peer sentinel"""
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
    app.state.remote_peers = RemotePeers(self_peer=RemoteSentinelPeer(config.host, config.port))
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
        app.state.remote_peers.add_peer(peer)

    start_background_task(app)
    
    yield

app = FastAPI(title="Health Check API", lifespan=lifespan)


# ===== DEPENDENCY FUNCTIONS =====

def get_cluster(request: Request) -> Cluster:
    """Provides access to the shared cluster instance"""
    return request.app.state.cluster


def get_peers(request: Request) -> RemotePeers:
    """Provides access to the list of sentinel peers"""
    return request.app.state.remote_peers


def get_config(request: Request) -> Config:
    """Provides access to the application configuration"""
    return request.app.state.config


# ===== ENDPOINTS =====

@app.post("/instance-down")
async def instance_down_notification(
    payload: App,
    cluster: Cluster = Depends(get_cluster),
    remote_peers: RemotePeers = Depends(get_peers)
):
    """
    Receive notification that an instance is down from a peer sentinel.
    """
    host = payload.host
    port = payload.port

    if not host or not port:
        return JSONResponse(
            content={"error": "Missing host or port"},
            status_code=400
        )
    
    instance = cluster.get_instance(host, port)
    if instance:
        await instance.add_remote_down(remote_peers)
        logger.info(f"Received notification: {host}:{port} is down (count: {instance.down_count()})")
        return JSONResponse(
            content={"message": "Instance marked as down"},
            status_code=200
        )
    
    return JSONResponse(
        content={"error": "Instance not found"},
        status_code=404
    )
    
@app.post("/instance-up")
async def instance_up_notification(
    payload: App,
    cluster: Cluster = Depends(get_cluster),
    remote_peers: RemotePeers = Depends(get_peers)
):
    """
    Receive notification that an instance is up from a peer sentinel.
    """
    host = payload.host
    port = payload.port

    if not host or not port:
        return JSONResponse(
            content={"error": "Missing host or port"},
            status_code=400
        )
    
    instance = cluster.get_instance(host, port)
    if instance:
        await instance.add_remote_up(remote_peers)
        logger.info(f"Received notification: {host}:{port} is up (count: {instance.down_count()})")
        return JSONResponse(
            content={"message": "Instance marked as up"},
            status_code=200
        )
    
    return JSONResponse(
        content={"error": "Instance not found"},
        status_code=404
    )
    


@app.post("/discover-peer")
async def discover_peer(
    payload: NewRemotePeer,
    cluster: Cluster = Depends(get_cluster),
    remote_peers: RemotePeers = Depends(get_peers),
):
    """
    Handle peer discovery requests.
    Exchange cluster information with the requesting peer.
    """

    remote_peer = RemoteSentinelPeer(payload.host, payload.port)
    new_instances = payload.local_instances
    logger.info(f"{len(new_instances)} instances received from peer {remote_peer.host}:{remote_peer.port}")

    if not remote_peer.host or not remote_peer.port:
        return JSONResponse(
            content={"error": "Invalid data: missing host or port"},
            status_code=400
        )

    remote_peers.add_peer(remote_peer)

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
    instances = cluster.get_instances()
    health_results = {str(instance): instance.state() for instance in instances}
    return {
        "total_instances": len(cluster.instances),
        "health_status": health_results
    }


@app.get("/peers")
async def list_peers(sentinel_cluster: RemotePeers = Depends(get_peers)):
    """List all known sentinel peers"""
    return {
        "total_peers": len(sentinel_cluster.get_peers()),
        "peers": [f"{peer.host}:{peer.port}" for peer in sentinel_cluster.get_peers()]
    }


@app.get("/health", tags=["Health"])
async def health_check():
    await asyncio.sleep(0.1)
    return JSONResponse(
        content={"status": "ok", "message": "Service is healthy"}
    )

@app.post("/request-failover")
async def request_failover(
    payload: App,
    remote_peers: RemotePeers = Depends(get_peers)
):
    """Endpoint to request failover from this sentinel's peers"""
    await remote_peers.requested_failover(payload)
    return JSONResponse(
        content={"message": "Failover requested"},
        status_code=200
    )

@app.post("/failover")
async def initiate_failover(
    payload: App,
    request: Request,
    remote_peers: RemotePeers = Depends(get_peers),
):
    if request.app.state.failover_in_progress:
        return JSONResponse(
            content={"message": "Failover already in progress"},
            status_code=200
        )
    logger.info(f"Failover initiated for instance {payload.host}:{payload.port}")
    request.app.state.failover_in_progress = True
    
    # Perform failover logic here
    failover_info = FailoverInfo(
        responsible_peer=App(host=request.app.state.config.host, port=request.app.state.config.port),
        failed_instance=payload
    )
    
    logger.info(f"Failover finished")
    await remote_peers.notify_failover_complete(failover_info)

@app.post("/failover-complete")
async def failover_complete(
    payload: FailoverInfo,
    remote_peers: RemotePeers = Depends(get_peers),
):
    """Endpoint to handle failover completion notifications from peers"""
    await remote_peers.update_after_failover(payload)
    logger.info(f"Failover completed with info: {payload}")
    return JSONResponse(
        content={"message": "Failover completion acknowledged"},
        status_code=200
    )


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "sentinel/config.yaml"
    config = load_config(config_file)
    uvicorn.run(app, host="0.0.0.0", port=config.port)