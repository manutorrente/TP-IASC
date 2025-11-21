import asyncio
from fastapi import APIRouter, Depends
from models.api import HealthResponse, ClusterStatusResponse, InstanceShards, PeersResponse, InstanceHealth
from models.domain import Cluster, ShardRole
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

    shards_info = []
    for instance in instances:
        master_shards = [shard.shard_id for shard in instance.shards if shard.role == ShardRole.MASTER]
        replica_shards = [shard.shard_id for shard in instance.shards if shard.role != ShardRole.MASTER]
        shards_info.append(
            InstanceShards(
                instance=str(instance),
                master_shards=master_shards,
                replica_shards=replica_shards
            )
        )

    return ClusterStatusResponse(
        total_instances=len(cluster.instances),
        health_status=health_status,
        shards=shards_info
    )


@router.get("/peers", response_model=PeersResponse, tags=["Health"])
async def list_peers(peer_service: PeerService = Depends(get_peer_service)):
    return PeersResponse(
        total_peers=len(peer_service.get_peers()),
        peers=[f"{peer.host}:{peer.port}" for peer in peer_service.get_peers()],
        offline_peers=[f"{peer.host}:{peer.port}" for peer in peer_service.offline_peers],
        coordinator=f"{peer_service.objective_coordinator.host}:{peer_service.objective_coordinator.port}" if peer_service.objective_coordinator else None
    )
