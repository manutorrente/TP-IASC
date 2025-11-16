from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from models import Operator, Window, CreateWindowRequest, Resource
from storage import Storage
from services.window_service import WindowService
from dependencies import get_storage, get_window_service
from datetime import datetime, timedelta
import uuid
import logging

logger = logging.getLogger(__name__)

router = APIRouter()



@router.post("/", response_model=Operator)
async def create_operator(name: str, email: str, storage: Storage = Depends(get_storage)):
    operator_id = str(uuid.uuid4())
    logger.info(f"Creating operator: {operator_id} - {name} ({email})")
    operator = Operator(
        id=operator_id,
        name=name,
        email=email
    )
    success = await storage.operators.add(operator)
    if not success:
        logger.error(f"Failed to create operator: {operator_id}")
        raise HTTPException(status_code=500, detail="Failed to replicate write to cluster")
    return operator

@router.get("/{operator_id}", response_model=Operator)
async def get_operator(operator_id: str, storage: Storage = Depends(get_storage)):
    logger.info(f"Fetching operator: {operator_id}")
    operator = await storage.operators.get(operator_id)
    if not operator:
        logger.warning(f"Operator not found: {operator_id}")
        raise HTTPException(status_code=404, detail="Operator not found")
    return operator

@router.post("/{operator_id}/windows", response_model=Window)
async def create_window(
    operator_id: str, 
    request: CreateWindowRequest,
    storage: Storage = Depends(get_storage),
    window_service: WindowService = Depends(get_window_service)
):
    logger.info(f"Creating window for operator: {operator_id} - Satellite: {request.satellite_name}")
    operator = await storage.operators.get(operator_id)
    if not operator:
        logger.warning(f"Operator not found when creating window: {operator_id}")
        raise HTTPException(status_code=404, detail="Operator not found")
    
    resources = [Resource(type=rt) for rt in request.resources]
    
    closes_at = datetime.utcnow() + timedelta(minutes=request.offer_duration_minutes)
    
    window_id = str(uuid.uuid4())
    window = Window(
        id=window_id,
        operator_id=operator_id,
        satellite_name=request.satellite_name,
        satellite_type=request.satellite_type,
        resources=resources,
        window_datetime=request.window_datetime,
        offer_duration_minutes=request.offer_duration_minutes,
        location=request.location,
        closes_at=closes_at
    )
    
    success = await window_service.create_window(window)
    if not success:
        logger.error(f"Failed to create window: {window_id}")
        raise HTTPException(status_code=500, detail="Failed to replicate write to cluster")
    logger.info(f"Window created successfully: {window_id}")
    
    return window