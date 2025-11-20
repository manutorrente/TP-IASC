import logging
from fastapi import APIRouter, Depends, HTTPException, status
from models.api import MasterNotification
from models.domain import Cluster
from services.peer_service import PeerService
from core.dependencies import get_cluster, get_peer_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/elect-master", status_code=status.HTTP_200_OK)
async def elect_master(
    peer_service: PeerService = Depends(get_peer_service),
    cluster: Cluster = Depends(get_cluster)
):
    await peer_service.elect_master(cluster)
    return {"message": "Master election initiated"}


@router.post("/new-master", status_code=status.HTTP_200_OK)
async def new_master(
    payload: MasterNotification,
    cluster: Cluster = Depends(get_cluster)
):
    host = payload.address.host
    port = int(payload.address.port)
    master_instance = cluster.get_instance(host, port)
    if master_instance:
        cluster.master = master_instance
        logger.info(f"New master instance set: {host}:{port}")
        return {"message": "New master instance set"}
    else:
        logger.warning(f"Instance {host}:{port} not found in cluster to set as master.")
        raise HTTPException(status_code=404, detail="Instance not found in cluster")
