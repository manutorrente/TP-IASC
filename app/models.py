from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from enum import Enum

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

class ReservationStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

class WindowStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"

class AlertCriteria(BaseModel):
    satellite_types: Optional[List[SatelliteType]] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    location: Optional[str] = None

class Alert(BaseModel):
    id: str
    user_id: str
    criteria: AlertCriteria
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Resource(BaseModel):
    type: ResourceType
    available: bool = True
    reserved_by: Optional[str] = None

class Window(BaseModel):
    id: str
    operator_id: str
    satellite_name: str
    satellite_type: SatelliteType
    resources: List[Resource]
    window_datetime: datetime
    offer_duration_minutes: int
    location: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    status: WindowStatus = WindowStatus.OPEN
    closes_at: datetime

class Reservation(BaseModel):
    id: str
    window_id: str
    user_id: str
    selected_resource: Optional[ResourceType] = None
    status: ReservationStatus = ReservationStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Operator(BaseModel):
    id: str
    name: str
    email: str

class User(BaseModel):
    id: str
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

class CreateAlertRequest(BaseModel):
    criteria: AlertCriteria

class CreateReservationRequest(BaseModel):
    window_id: str

class SelectResourceRequest(BaseModel):
    resource_type: ResourceType

class Notification(BaseModel):
    id: str
    user_id: str
    message: str
    window_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    read: bool = False