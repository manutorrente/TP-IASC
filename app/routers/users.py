from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from models import User, Alert, CreateAlertRequest, Notification
from storage import Storage
from dependencies import get_storage
from typing import List
import logging 

logger = logging.getLogger(__name__)

router = APIRouter()

class UserCreationRequest(BaseModel):
    user_id: str
    name: str
    email: str
    organization: str

@router.post("", response_model=User)
async def create_user(request: UserCreationRequest, storage: Storage = Depends(get_storage)):
    try:
        logger.info(f"Creating user with ID: {request.user_id}")
        user = User(
            id=request.user_id,
            name=request.name,
            email=request.email,
            organization=request.organization
        )
        logger.debug(f"User object created: {user}")
        success = await storage.users.add(user)
        if not success:
            logger.error(f"Failed to add user {request.user_id} to storage")
            raise HTTPException(status_code=500, detail="Failed to replicate write to cluster")
        logger.info(f"User created successfully: {request.user_id}")
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception occurred while creating user {request.user_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("", response_model=List[User])
async def get_all_users(storage: Storage = Depends(get_storage)):
    users = list(storage.users._data.values())
    return users

@router.get("/{user_id}", response_model=User)
async def get_user(user_id: str, storage: Storage = Depends(get_storage)):
    user = await storage.users.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

class CreateAlertRequestWithId(BaseModel):
    alert_id: str  # Now received from load balancer
    criteria: dict  # Comes as dict from load balancer

@router.post("/{user_id}/alerts", response_model=Alert)
async def create_alert(user_id: str, request: CreateAlertRequestWithId, storage: Storage = Depends(get_storage)):
    user = await storage.users.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    from models import AlertCriteria
    criteria = AlertCriteria(**request.criteria)
    
    alert = Alert(
        id=request.alert_id,  # Use ID from load balancer
        user_id=user_id,
        criteria=criteria
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