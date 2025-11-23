import logging
import asyncio
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from models.api import App
from models.domain import Cluster
from services.peer_service import PeerService
from core.dependencies import get_cluster, get_peer_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/instance-down", status_code=status.HTTP_200_OK)
async def instance_down_notification(
    payload: App,
    background_tasks: BackgroundTasks,
    cluster: Cluster = Depends(get_cluster),
    peer_service: PeerService = Depends(get_peer_service)
):
    host = payload.host
    port = payload.port

    if not host or not port:
        raise HTTPException(status_code=400, detail="Missing host or port")

    instance = cluster.get_instance(host, port)
    if instance:
        # Run the failover process in background to return immediately
        background_tasks.add_task(instance.add_remote_down, peer_service)
        logger.info(f"Received notification: {host}:{port} is down, processing in background")
        return {"message": "Instance down notification acknowledged"}

    raise HTTPException(status_code=404, detail="Instance not found")


@router.post("/instance-up", status_code=status.HTTP_200_OK)
async def instance_up_notification(
    payload: App,
    background_tasks: BackgroundTasks,
    cluster: Cluster = Depends(get_cluster),
    peer_service: PeerService = Depends(get_peer_service)
):
    host = payload.host
    port = payload.port

    if not host or not port:
        raise HTTPException(status_code=400, detail="Missing host or port")

    instance = cluster.get_instance(host, port)
    if instance:
        # Run the update in background to return immediately
        background_tasks.add_task(instance.add_remote_up, peer_service)
        logger.info(f"Received notification: {host}:{port} is up, processing in background")
        return {"message": "Instance up notification acknowledged"}

    raise HTTPException(status_code=404, detail="Instance not found")
