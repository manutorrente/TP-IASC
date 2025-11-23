import asyncio
from models import Reservation, Window, ResourceType, ReservationStatus, WindowStatus
import logging
from storage import Storage
from cluster.cluster_manager import cluster_manager

logger = logging.getLogger(__name__)

class ReservationService:
    def __init__(self, storage=None, window_service=None, notification_service=None):
        logger.debug("Initializing ReservationService")
        self._window_resource_locks: dict[str, asyncio.Lock] = {}
        self._storage: Storage = storage
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
                logger.debug(f"Checking resource: {resource.type}, available: {resource.available}")
                if resource.type == resource_type and resource.available:
                    target_resource = resource
                    break
            
            if not target_resource:
                available_resources = [r for r in window.resources if r.available]
                logger.warning(f"Target resource not available: {resource_type} for reservation: {reservation_id}")
                logger.warning(f"  Available resources: {[r.type for r in available_resources]}")
                logger.warning(f"  All resources: {[(r.type, r.available) for r in window.resources]}")
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
                raise ValueError(f"Selected resource not available: {resource_type}")
            
            target_resource.available = False
            target_resource.reserved_by = reservation.user_id
            logger.debug(f"Resource reserved for user: {reservation.user_id}")
            
            reservation.selected_resource = resource_type
            reservation.status = ReservationStatus.COMPLETED
            
            # Determine which nodes are masters for each entity
            window_is_local_master = cluster_manager._is_master_for_entity(window.id)
            reservation_is_local_master = cluster_manager._is_master_for_entity(reservation_id)
            
            logger.debug(f"Window {window.id} master is local: {window_is_local_master}")
            logger.debug(f"Reservation {reservation_id} master is local: {reservation_is_local_master}")
            
            # Case 1: Both entities have this node as master
            if window_is_local_master and reservation_is_local_master:
                logger.debug("Both entities are local masters - using normal update flow")
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
            
            # Case 2: Entities have different masters
            # Strategy: Update local master first, then release lock and forward writes to remote masters
            logger.info(f"Cross-master write detected - Reservation master local: {reservation_is_local_master}, Window master local: {window_is_local_master}")
            
            # Update the entity we are master for
            if reservation_is_local_master:
                logger.debug(f"Updating reservation locally (we are master)")
                reservation_success = await self._storage.reservations.update(reservation)
                if not reservation_success:
                    logger.error(f"Failed to update reservation locally: {reservation_id}")
                    return None
            
            if window_is_local_master:
                logger.debug(f"Updating window locally (we are master)")
                window_success = await self._storage.windows.update(window)
                if not window_success:
                    logger.error(f"Failed to update window locally: {window.id}")
                    return None
            
            # Now release the lock BEFORE forwarding writes to remote masters
            # This prevents deadlocks when the remote master tries to send us prepare/commit requests
            logger.debug(f"Releasing lock for window {window.id} before forwarding to remote masters")
        
        # After releasing the lock, forward writes to remote masters if needed
        if not reservation_is_local_master:
            logger.info(f"Forwarding reservation write to remote master for {reservation_id}")
            reservation_data = reservation.model_dump(mode='json')
            reservation_success = await cluster_manager._send_write_to_master(
                reservation_id, "reservation", "update", reservation_data
            )
            if not reservation_success:
                logger.error(f"Failed to forward reservation write to master: {reservation_id}")
                raise ValueError("Failed to replicate reservation update to master node")
        
        if not window_is_local_master:
            logger.info(f"Forwarding window write to remote master for {window.id}")
            window_data = window.model_dump(mode='json')
            window_success = await cluster_manager._send_write_to_master(
                window.id, "window", "update", window_data
            )
            if not window_success:
                logger.error(f"Failed to forward window write to master: {window.id}")
                raise ValueError("Failed to replicate window update to master node")
        
        logger.info(f"Resource selected successfully with cross-master writes: {reservation_id}")
        
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