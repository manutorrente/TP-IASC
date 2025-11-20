import asyncio
from fastapi import APIRouter, Depends
from models.api import HealthResponse, ClusterStatusResponse, PeersResponse, InstanceHealth
from models.domain import Cluster
from services.peer_service import PeerService
from core.dependencies import get_cluster, get_peer_service

router = APIRouter()


@router.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    await asyncio.sleep(0.1)
    return HealthResponse(status="ok", message="Service is healthy")


@router.get("/cluster-status", response_model=ClusterStatusResponse, tags=["Health"])
async def cluster_status(
    cluster: Cluster = Depends(get_cluster),
    peer_service: PeerService = Depends(get_peer_service)
):
    if cluster.master is None:
        await peer_service.elect_master(cluster)

    instances = cluster.get_instances()
    health_status = {
        str(instance): InstanceHealth(**instance.state())
        for instance in instances
    }

    return ClusterStatusResponse(
        total_instances=len(cluster.instances),
        health_status=health_status,
        master=str(cluster.master) if cluster.master else None
    )


@router.get("/peers", response_model=PeersResponse, tags=["Health"])
async def list_peers(peer_service: PeerService = Depends(get_peer_service)):
    return PeersResponse(
        total_peers=len(peer_service.get_peers()),
        peers=[f"{peer.host}:{peer.port}" for peer in peer_service.get_peers()],
        offline_peers=[f"{peer.host}:{peer.port}" for peer in peer_service.offline_peers],
        coordinator=f"{peer_service.objective_coordinator.host}:{peer_service.objective_coordinator.port}" if peer_service.objective_coordinator else None
    )
