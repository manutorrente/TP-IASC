import logging
from fastapi import APIRouter, Depends, HTTPException, status
from models.api import PeerDiscoveryRequest, PeerDiscoveryResponse, CoordinatorUpdate, CoordinatorChange
from models.domain import Cluster
from services.peer_service import PeerService, RemoteSentinelPeer
from core.dependencies import get_cluster, get_peer_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/discover-peer", response_model=PeerDiscoveryResponse, status_code=status.HTTP_200_OK)
async def discover_peer(
    payload: PeerDiscoveryRequest,
    cluster: Cluster = Depends(get_cluster),
    peer_service: PeerService = Depends(get_peer_service),
):
    remote_peer = RemoteSentinelPeer(payload.host, payload.port)
    new_instances = payload.local_instances
    logger.info(f"{len(new_instances)} instances received from peer {remote_peer.host}:{remote_peer.port}")

    if not remote_peer.host or not remote_peer.port:
        raise HTTPException(status_code=400, detail="Invalid data: missing host or port")

    peer_service.add_peer(remote_peer)
    await cluster.add_instances(new_instances)

    local_cluster_info = cluster.get_instances_list()

    return PeerDiscoveryResponse(
        message="Peer discovered",
        local_instances=local_cluster_info
    )


@router.post("/coordinator-update", status_code=status.HTTP_200_OK)
async def coordinator_update(
    payload: CoordinatorUpdate,
    peer_service: PeerService = Depends(get_peer_service),
):
    coordinator_choice = payload.coordinator_pick.address
    await peer_service.incoming_coordinator_update(
        remote_coordinator_pick=coordinator_choice,
        origin=payload.origin.address
    )
    return {"message": "Coordinator update processed"}


@router.post("/coordinator-change", status_code=status.HTTP_200_OK)
async def coordinator_change(
    payload: CoordinatorChange,
    peer_service: PeerService = Depends(get_peer_service),
):
    peer_service.change_coordinator(payload.address)
    return {"message": "Coordinator change acknowledged"}


