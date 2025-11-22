from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from models import Reservation, CreateReservationRequest, SelectResourceRequest
from storage import Storage
from services.reservation_service import ReservationService
from dependencies import get_storage, get_reservation_service
from typing import List
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

class CreateReservationRequestWithId(BaseModel):
    reservation_id: str  # Now received from load balancer
    window_id: str

@router.post("", response_model=Reservation)
async def create_reservation(
    user_id: str, 
    request: CreateReservationRequestWithId,
    storage: Storage = Depends(get_storage),
    reservation_service: ReservationService = Depends(get_reservation_service)
):
    logger.info(f"Creating reservation - User: {user_id}, Window: {request.window_id}")
    user = await storage.users.get(user_id)
    if not user:
        logger.warning(f"User not found: {user_id}")
        raise HTTPException(status_code=404, detail="User not found")
    
    reservation = Reservation(
        id=request.reservation_id,  # Use ID from load balancer
        window_id=request.window_id,
        user_id=user_id
    )
    
    try:
        result = await reservation_service.create_reservation(reservation)
        if not result:
            logger.error(f"Failed to create reservation: {request.reservation_id}")
            raise HTTPException(status_code=500, detail="Failed to replicate write to cluster")
        logger.info(f"Reservation created successfully: {request.reservation_id}")
        return result
    except ValueError as e:
        logger.warning(f"Invalid reservation request: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/{reservation_id}", response_model=Reservation)
async def get_reservation(reservation_id: str, storage: Storage = Depends(get_storage)):
    logger.info(f"Fetching reservation: {reservation_id}")
    reservation = await storage.reservations.get(reservation_id)
    if not reservation:
        logger.warning(f"Reservation not found: {reservation_id}")
        raise HTTPException(status_code=404, detail="Reservation not found")
    return reservation

@router.get("/user/{user_id}", response_model=List[Reservation])
async def get_user_reservations(user_id: str, storage: Storage = Depends(get_storage)):
    logger.info(f"Fetching reservations for user: {user_id}")
    user = await storage.users.get(user_id)
    if not user:
        logger.warning(f"User not found: {user_id}")
        raise HTTPException(status_code=404, detail="User not found")
    
    reservations = await storage.reservations.get_by_user(user_id)
    logger.info(f"Retrieved {len(reservations)} reservations for user: {user_id}")
    return reservations

@router.post("/{reservation_id}/select-resource", response_model=Reservation)
async def select_resource(
    reservation_id: str, 
    request: SelectResourceRequest,
    reservation_service: ReservationService = Depends(get_reservation_service)
):
    logger.info(f"Selecting resource for reservation: {reservation_id} - Resource: {request.resource_type}")
    try:
        reservation = await reservation_service.select_resource(reservation_id, request.resource_type)
        if not reservation:
            logger.error(f"Failed to select resource for reservation: {reservation_id}")
            raise HTTPException(status_code=500, detail="Failed to replicate write to cluster")
        logger.info(f"Resource selected successfully for reservation: {reservation_id}")
        return reservation
    except ValueError as e:
        logger.warning(f"Invalid resource selection: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{reservation_id}/cancel", response_model=Reservation)
async def cancel_reservation(
    reservation_id: str,
    storage: Storage = Depends(get_storage),
    reservation_service: ReservationService = Depends(get_reservation_service)
):
    logger.info(f"Cancelling reservation: {reservation_id}")
    try:
        success = await reservation_service.cancel_reservation(reservation_id)
        if not success:
            logger.error(f"Failed to cancel reservation: {reservation_id}")
            raise HTTPException(status_code=500, detail="Failed to replicate write to cluster")
        reservation = await storage.reservations.get(reservation_id)
        logger.info(f"Reservation cancelled successfully: {reservation_id}")
        return reservation
    except ValueError as e:
        logger.warning(f"Invalid cancellation request: {e}")
        raise HTTPException(status_code=400, detail=str(e))