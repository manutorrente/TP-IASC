import asyncio
from datetime import datetime, timedelta
from models import Window, WindowStatus, ReservationStatus
import logging

logger = logging.getLogger(__name__)

class WindowService:
    def __init__(self, storage=None, notification_service=None):
        logger.debug("Initializing WindowService")
        self._storage = storage
        self._notification_service = notification_service
    
    def set_storage(self, storage):
        logger.debug("Setting storage for WindowService")
        self._storage = storage
    
    def set_notification_service(self, notification_service):
        logger.debug("Setting notification service for WindowService")
        self._notification_service = notification_service
    
    async def monitor_windows(self):
        logger.info("Starting window monitoring loop")
        while True:
            try:
                await asyncio.sleep(1)
                await self._check_expired_windows()
            except asyncio.CancelledError:
                logger.info("Window monitoring loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error monitoring windows: {e}", exc_info=True)
    
    async def _check_expired_windows(self):
        windows = await self._storage.windows.get_all()
        current_time = datetime.utcnow()
        
        for window in windows:
            if window.status == WindowStatus.OPEN:
                if current_time >= window.closes_at:
                    logger.info(f"Closing expired window: {window.id} - Offer time expired")
                    await self.close_window(window.id, "offer time expired")
                elif await self._all_resources_occupied(window):
                    logger.info(f"Closing window with all resources occupied: {window.id}")
                    await self.close_window(window.id, "all resources occupied")
    
    async def _all_resources_occupied(self, window: Window) -> bool:
        return all(not resource.available for resource in window.resources)
    
    async def close_window(self, window_id: str, reason: str):
        logger.info(f"Closing window {window_id}: {reason}")
        window = await self._storage.windows.get(window_id)
        if not window or window.status == WindowStatus.CLOSED:
            logger.debug(f"Window already closed or not found: {window_id}")
            return
        
        window.status = WindowStatus.CLOSED
        success = await self._storage.windows.update(window)
        if not success:
            logger.error(f"Failed to update window status for {window_id}")
            return
        
        logger.info(f"Notifying users about window closure: {window_id}")
        reservations = await self._storage.reservations.get_by_window(window_id)
        
        for reservation in reservations:
            if reservation.status == ReservationStatus.PENDING:
                reservation.status = ReservationStatus.CANCELLED
                await self._storage.reservations.update(reservation)
                await self._notification_service.notify_reservation_cancelled(
                    reservation.user_id,
                    window_id,
                    reason
                )
        
        await self._notification_service.notify_window_closed(window_id)
        logger.info(f"Window closed successfully: {window_id}")
    
    async def create_window(self, window: Window) -> bool:
        logger.info(f"Creating window: {window.id}")
        success = await self._storage.windows.add(window)
        if not success:
            logger.error(f"Failed to add window: {window.id}")
            return False
        await self._notification_service.notify_new_window(window)
        logger.info(f"Window created and notifications sent: {window.id}")
        return True
    
    async def get_available_resources(self, window_id: str):
        window = await self._storage.windows.get(window_id)
        if not window:
            logger.warning(f"Window not found: {window_id}")
            return []
        available = [r for r in window.resources if r.available]
        logger.debug(f"Retrieved {len(available)} available resources for window {window_id}")
        return available

window_service = WindowService()