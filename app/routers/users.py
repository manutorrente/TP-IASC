from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from models import User, Alert, CreateAlertRequest, Notification
from storage import Storage
from dependencies import get_storage
from typing import List
import uuid
import logging 

logger = logging.getLogger(__name__)

router = APIRouter()

class UserCreationRequest(BaseModel):
    name: str
    email: str
    organization: str

@router.post("/", response_model=User)
async def create_user(request: UserCreationRequest, storage: Storage = Depends(get_storage)):
    id=str(uuid.uuid4())
    logger.info(f"Creating user with ID: {id}")
    user = User(
        id=id,
        name=request.name,
        email=request.email,
        organization=request.organization
    )
    success = await storage.users.add(user)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to replicate write to cluster")
    return user

@router.get("/", response_model=List[User])
async def get_all_users(storage: Storage = Depends(get_storage)):
    users = list(storage.users._data.values())
    return users

@router.get("/{user_id}", response_model=User)
async def get_user(user_id: str, storage: Storage = Depends(get_storage)):
    user = await storage.users.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.post("/{user_id}/alerts", response_model=Alert)
async def create_alert(user_id: str, request: CreateAlertRequest, storage: Storage = Depends(get_storage)):
    user = await storage.users.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    alert = Alert(
        id=str(uuid.uuid4()),
        user_id=user_id,
        criteria=request.criteria
    )
    
    success = await storage.alerts.add(user_id, alert)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to replicate write to cluster")
    return alert

@router.get("/{user_id}/alerts", response_model=List[Alert])
async def get_user_alerts(user_id: str, storage: Storage = Depends(get_storage)):
    user = await storage.users.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    alerts = await storage.alerts.get_by_user(user_id)
    return alerts

@router.get("/{user_id}/notifications", response_model=List[Notification])
async def get_user_notifications(user_id: str, storage: Storage = Depends(get_storage)):
    user = await storage.users.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    notifications = await storage.notifications.get_by_user(user_id)
    return notifications