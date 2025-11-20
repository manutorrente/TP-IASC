import logging
from fastapi import APIRouter, Depends, status
from models.api import FailoverNotification
from services.peer_service import PeerService
from core.dependencies import get_peer_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/failover-start", status_code=status.HTTP_200_OK)
async def failover_start(
    payload: FailoverNotification,
    peer_service: PeerService = Depends(get_peer_service),
):
    logger.info(f"Failover started with info: {payload}")
    return {"message": "Failover start acknowledged"}


@router.post("/failover-complete", status_code=status.HTTP_200_OK)
async def failover_complete(
    payload: FailoverNotification,
    peer_service: PeerService = Depends(get_peer_service),
):
    logger.info(f"Failover completed with info: {payload}")
    return {"message": "Failover completion acknowledged"}
