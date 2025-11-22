import asyncio
from fastapi import APIRouter, Depends, Query
from models.api import HealthResponse, ClusterStatusResponse, InstanceShards, PeersResponse, InstanceHealth
from models.domain import Cluster, ShardRole
from services.peer_service import PeerService
from core.dependencies import get_cluster, get_peer_service
from pydantic import BaseModel

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


class ShardMasterResponse(BaseModel):
    node_address: str
    num_shards: int

class ShardMasterRequest(BaseModel):
    shard_id: int

@router.get("/shard-master", response_model=ShardMasterResponse, tags=["Health"])
async def get_shard_master(
    shard_id: int = Query(...),
    cluster: Cluster = Depends(get_cluster)
):
    return ShardMasterResponse(
        node_address=str(cluster._get_master_for_shard(shard_id)),
        num_shards=cluster.n_shard_partitions()
    )
    
class ShardSlavesResponse(BaseModel):
    slave_addresses: list[str]
    
@router.get("/shard-slaves", response_model=ShardSlavesResponse, tags=["Health"])
async def get_shard_slaves(
    shard_id: int = Query(...),
    cluster: Cluster = Depends(get_cluster)
):
    slave_instances = cluster._get_slaves_for_shard(shard_id)
    return ShardSlavesResponse(
        slave_addresses=[str(instance) for instance in slave_instances]
    )