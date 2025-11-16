import asyncio
from models import Reservation, Window, ResourceType, ReservationStatus, WindowStatus
import logging

logger = logging.getLogger(__name__)

class ReservationService:
    def __init__(self, storage=None, window_service=None, notification_service=None):
        logger.debug("Initializing ReservationService")
        self._window_resource_locks: dict[str, asyncio.Lock] = {}
        self._storage = storage
        self._window_service = window_service
        self._notification_service = notification_service
    
    def set_storage(self, storage):
        logger.debug("Setting storage for ReservationService")
        self._storage = storage
    
    def set_window_service(self, window_service):
        logger.debug("Setting window service for ReservationService")
        self._window_service = window_service
    
    def set_notification_service(self, notification_service):
        logger.debug("Setting notification service for ReservationService")
        self._notification_service = notification_service
    
    async def _get_window_resource_lock(self, window_id: str) -> asyncio.Lock:
        if window_id not in self._window_resource_locks:
            self._window_resource_locks[window_id] = asyncio.Lock()
        return self._window_resource_locks[window_id]
    
    async def create_reservation(self, reservation: Reservation) -> Reservation:
        logger.info(f"Creating reservation: {reservation.id} - Window: {reservation.window_id}, User: {reservation.user_id}")
        window = await self._storage.windows.get(reservation.window_id)
        
        if not window:
            logger.warning(f"Window not found: {reservation.window_id}")
            raise ValueError("Window not found")
        
        if window.status == WindowStatus.CLOSED:
            logger.warning(f"Cannot create reservation for closed window: {reservation.window_id}")
            raise ValueError("Window is closed")
        
        success = await self._storage.reservations.add(reservation)
        if not success:
            logger.error(f"Failed to add reservation: {reservation.id}")
            return None
        logger.info(f"Reservation created successfully: {reservation.id}")
        return reservation
    
    async def select_resource(self, reservation_id: str, resource_type: ResourceType) -> Reservation:
        logger.info(f"Selecting resource for reservation: {reservation_id} - Resource type: {resource_type}")
        reservation = await self._storage.reservations.get(reservation_id)
        
        if not reservation:
            logger.warning(f"Reservation not found: {reservation_id}")
            raise ValueError("Reservation not found")
        
        if reservation.status != ReservationStatus.PENDING:
            logger.warning(f"Reservation is not pending: {reservation_id} - Status: {reservation.status}")
            raise ValueError("Reservation is not pending")
        
        window = await self._storage.windows.get(reservation.window_id)
        
        if not window:
            logger.warning(f"Window not found: {reservation.window_id}")
            raise ValueError("Window not found")
        
        if window.status == WindowStatus.CLOSED:
            logger.warning(f"Window is closed: {reservation.window_id}")
            raise ValueError("Window is closed")
        
        resource_lock = await self._get_window_resource_lock(window.id)
        async with resource_lock:
            logger.debug(f"Acquired lock for window: {window.id}")
            window = await self._storage.windows.get(reservation.window_id)
            
            target_resource = None
            for resource in window.resources:
                if resource.type == resource_type and resource.available:
                    target_resource = resource
                    break
            
            if not target_resource:
                available_resources = [r for r in window.resources if r.available]
                if not available_resources:
                    logger.warning(f"No resources available for reservation: {reservation_id}")
                    reservation.status = ReservationStatus.CANCELLED
                    await self._storage.reservations.update(reservation)
                    await self._notification_service.notify_reservation_cancelled(
                        reservation.user_id,
                        window.id,
                        "no resources available"
                    )
                    raise ValueError("No resources available. Your reservation has been cancelled.")
                logger.warning(f"Selected resource not available: {resource_type}")
                raise ValueError("Selected resource not available")
            
            target_resource.available = False
            target_resource.reserved_by = reservation.user_id
            logger.debug(f"Resource reserved for user: {reservation.user_id}")
            
            reservation.selected_resource = resource_type
            reservation.status = ReservationStatus.COMPLETED
            
            window_success = await self._storage.windows.update(window)
            reservation_success = await self._storage.reservations.update(reservation)
            
            if not window_success or not reservation_success:
                logger.error(f"Failed to update window or reservation during resource selection: {reservation_id}")
                return None
            logger.info(f"Resource selected successfully: {reservation_id}")
            
            if await self._window_service._all_resources_occupied(window):
                logger.info(f"All resources occupied for window: {window.id}")
                asyncio.create_task(self._window_service.close_window(window.id, "all resources occupied"))
            
            return reservation
    
    async def cancel_reservation(self, reservation_id: str) -> bool:
        logger.info(f"Cancelling reservation: {reservation_id}")
        reservation = await self._storage.reservations.get(reservation_id)
        
        if not reservation:
            logger.warning(f"Reservation not found: {reservation_id}")
            raise ValueError("Reservation not found")
        
        if reservation.status == ReservationStatus.COMPLETED:
            logger.warning(f"Cannot cancel completed reservation: {reservation_id}")
            raise ValueError("Cannot cancel completed reservation")
        
        reservation.status = ReservationStatus.CANCELLED
        success = await self._storage.reservations.update(reservation)
        logger.info(f"Reservation cancelled successfully: {reservation_id}")
        return success

reservation_service = ReservationService()