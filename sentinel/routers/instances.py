import logging
from fastapi import APIRouter, Depends, HTTPException, status
from models.api import App
from models.domain import Cluster
from services.peer_service import PeerService
from core.dependencies import get_cluster, get_peer_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/instance-down", status_code=status.HTTP_200_OK)
async def instance_down_notification(
    payload: App,
    cluster: Cluster = Depends(get_cluster),
    peer_service: PeerService = Depends(get_peer_service)
):
    host = payload.host
    port = payload.port

    if not host or not port:
        raise HTTPException(status_code=400, detail="Missing host or port")

    instance = cluster.get_instance(host, port)
    if instance:
        await instance.add_remote_down(peer_service)
        logger.info(f"Received notification: {host}:{port} is down (count: {instance.down_count()})")
        return {"message": "Instance marked as down"}

    raise HTTPException(status_code=404, detail="Instance not found")


@router.post("/instance-up", status_code=status.HTTP_200_OK)
async def instance_up_notification(
    payload: App,
    cluster: Cluster = Depends(get_cluster),
    peer_service: PeerService = Depends(get_peer_service)
):
    host = payload.host
    port = payload.port

    if not host or not port:
        raise HTTPException(status_code=400, detail="Missing host or port")

    instance = cluster.get_instance(host, port)
    if instance:
        await instance.add_remote_up(peer_service)
        logger.info(f"Received notification: {host}:{port} is up (count: {instance.down_count()})")
        return {"message": "Instance marked as up"}

    raise HTTPException(status_code=404, detail="Instance not found")
