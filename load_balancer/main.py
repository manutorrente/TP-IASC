from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from enum import Enum
import uuid
import httpx
from conf import sentinels
import jump
from utils import *
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

# Enums (copied from models.py)
class SatelliteType(str, Enum):
    OPTICAL_OBSERVATION = "optical_observation"
    RADAR = "radar"
    COMMUNICATIONS = "communications"
    EXPERIMENTAL = "experimental"

class ResourceType(str, Enum):
    OPTICAL_CHANNEL = "optical_channel"
    RADAR_CHANNEL = "radar_channel"
    DATA_TRANSMISSION = "data_transmission"
    SENSOR_A = "sensor_a"
    SENSOR_B = "sensor_b"

# Request models
class UserCreationRequest(BaseModel):
    name: str
    email: str
    organization: str

class CreateWindowRequest(BaseModel):
    satellite_name: str
    satellite_type: SatelliteType
    resources: List[ResourceType]
    window_datetime: datetime
    offer_duration_minutes: int
    location: Optional[str] = None

class AlertCriteria(BaseModel):
    satellite_types: Optional[List[SatelliteType]] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    location: Optional[str] = None

class CreateAlertRequest(BaseModel):
    criteria: AlertCriteria

class CreateReservationRequest(BaseModel):
    window_id: str

class SelectResourceRequest(BaseModel):
    resource_type: ResourceType



# User endpoints
@app.post("/users")
async def create_user(request: UserCreationRequest):
    user_id = str(uuid.uuid4())
    logger.info(f"Creating user with ID: {user_id}, name: {request.name}, email: {request.email}")
    try:
        response = await forward_request_to_master(
            user_id,
            "POST",
            "/users",
            json={
                "user_id": user_id,
                "name": request.name,
                "email": request.email,
                "organization": request.organization
            }
        )
        logger.info(f"Successfully created user {user_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to create user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users")
async def get_all_users():
    logger.info("Retrieving all users")
    try:
        response = await forward_request_to_master(
            str(uuid.uuid4()),  # Dummy ID to get a master
            "GET",
            "/users"
        )
        logger.info(f"Successfully retrieved all users, count: {len(response) if isinstance(response, list) else 'N/A'}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve all users: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    logger.info(f"Retrieving user: {user_id}")
    try:
        response = await forward_request_to_master(user_id, "GET", f"/users/{user_id}")
        logger.info(f"Successfully retrieved user {user_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve user {user_id}: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/users/{user_id}/alerts")
async def create_alert(user_id: str, request: CreateAlertRequest):
    alert_id = str(uuid.uuid4())
    logger.info(f"Creating alert {alert_id} for user {user_id}")
    try:
        response = await forward_request_to_master(
            alert_id,
            "POST",
            f"/users/{user_id}/alerts",
            json={"alert_id": alert_id, "criteria": request.criteria.model_dump(mode='json')}
        )
        logger.info(f"Successfully created alert {alert_id} for user {user_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to create alert for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/{user_id}/alerts")
async def get_user_alerts(user_id: str):
    logger.info(f"Retrieving alerts for user {user_id}")
    try:
        response = await forward_request_to_master(user_id, "GET", f"/users/{user_id}/alerts")
        logger.info(f"Successfully retrieved alerts for user {user_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve alerts for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/{user_id}/notifications")
async def get_user_notifications(user_id: str):
    logger.info(f"Retrieving notifications for user {user_id}")
    try:
        response = await forward_request_to_master(user_id, "GET", f"/users/{user_id}/notifications")
        logger.info(f"Successfully retrieved notifications for user {user_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve notifications for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Operator endpoints
@app.post("/operators")
async def create_operator(name: str, email: str):
    operator_id = str(uuid.uuid4())
    logger.info(f"Creating operator with ID: {operator_id}, name: {name}, email: {email}")
    try:
        response = await forward_request_to_master(
            operator_id,
            "POST",
            "/operators",
            params={"operator_id": operator_id, "name": name, "email": email}
        )
        logger.info(f"Successfully created operator {operator_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to create operator {operator_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/operators/{operator_id}")
async def get_operator(operator_id: str):
    logger.info(f"Retrieving operator: {operator_id}")
    try:
        response = await forward_request_to_master(operator_id, "GET", f"/operators/{operator_id}")
        logger.info(f"Successfully retrieved operator {operator_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve operator {operator_id}: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/operators/{operator_id}/windows")
async def create_window(operator_id: str, request: CreateWindowRequest):
    window_id = str(uuid.uuid4())
    logger.info(f"Creating window {window_id} for operator {operator_id}, satellite: {request.satellite_name}")
    try:
        response = await forward_request_to_master(
            window_id,
            "POST",
            f"/operators/{operator_id}/windows",
            json={"window_id": window_id, **request.model_dump(mode='json')}
        )
        logger.info(f"Successfully created window {window_id} for operator {operator_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to create window for operator {operator_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Window endpoints
@app.get("/windows")
async def get_all_windows():
    logger.info("Retrieving all windows")
    try:
        response = await forward_request_to_master(
            str(uuid.uuid4()),
            "GET",
            "/windows"
        )
        logger.info(f"Successfully retrieved all windows, count: {len(response) if isinstance(response, list) else 'N/A'}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve all windows: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/windows/{window_id}")
async def get_window(window_id: str):
    logger.info(f"Retrieving window: {window_id}")
    try:
        response = await forward_request_to_master(window_id, "GET", f"/windows/{window_id}")
        logger.info(f"Successfully retrieved window {window_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve window {window_id}: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/windows/{window_id}/resources")
async def get_window_resources(window_id: str):
    logger.info(f"Retrieving resources for window: {window_id}")
    try:
        response = await forward_request_to_master(window_id, "GET", f"/windows/{window_id}/resources")
        logger.info(f"Successfully retrieved resources for window {window_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve resources for window {window_id}: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/windows/{window_id}/available-resources")
async def get_available_resources(window_id: str):
    logger.info(f"Retrieving available resources for window: {window_id}")
    try:
        response = await forward_request_to_master(window_id, "GET", f"/windows/{window_id}/available-resources")
        logger.info(f"Successfully retrieved available resources for window {window_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve available resources for window {window_id}: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

# Reservation endpoints
@app.post("/reservations")
async def create_reservation(user_id: str, request: CreateReservationRequest):
    reservation_id = str(uuid.uuid4())
    logger.info(f"Creating reservation {reservation_id} for user {user_id}, window {request.window_id}")
    try:
        response = await forward_request_to_master(
            reservation_id,
            "POST",
            "/reservations",
            params={"user_id": user_id},
            json={"reservation_id": reservation_id, "window_id": request.window_id}
        )
        logger.info(f"Successfully created reservation {reservation_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to create reservation for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reservations/{reservation_id}")
async def get_reservation(reservation_id: str):
    logger.info(f"Retrieving reservation: {reservation_id}")
    try:
        response = await forward_request_to_master(reservation_id, "GET", f"/reservations/{reservation_id}")
        logger.info(f"Successfully retrieved reservation {reservation_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve reservation {reservation_id}: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/reservations/user/{user_id}")
async def get_user_reservations(user_id: str):
    logger.info(f"Retrieving reservations for user: {user_id}")
    try:
        response = await forward_request_to_master(user_id, "GET", f"/reservations/user/{user_id}")
        logger.info(f"Successfully retrieved reservations for user {user_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to retrieve reservations for user {user_id}: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/reservations/{reservation_id}/select-resource")
async def select_resource(reservation_id: str, request: SelectResourceRequest):
    logger.info(f"Selecting resource {request.resource_type} for reservation {reservation_id}")
    try:
        response = await forward_request_to_master(
            reservation_id,
            "POST",
            f"/reservations/{reservation_id}/select-resource",
            json=request.model_dump()
        )
        logger.info(f"Successfully selected resource for reservation {reservation_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to select resource for reservation {reservation_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reservations/{reservation_id}/cancel")
async def cancel_reservation(reservation_id: str):
    logger.info(f"Cancelling reservation: {reservation_id}")
    try:
        response = await forward_request_to_master(
            reservation_id,
            "POST",
            f"/reservations/{reservation_id}/cancel"
        )
        logger.info(f"Successfully cancelled reservation {reservation_id}")
        return response
    except Exception as e:
        logger.error(f"Failed to cancel reservation {reservation_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI application on port 7000")
    uvicorn.run(app, host="0.0.0.0", port=7000)