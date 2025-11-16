import asyncio
from typing import List
from datetime import datetime
from models import Window, Alert, Notification, AlertCriteria
import uuid
import logging

logger = logging.getLogger(__name__)

class NotificationService:
    def __init__(self, storage=None):
        logger.debug("Initializing NotificationService")
        self.notification_queue: asyncio.Queue = asyncio.Queue()
        self._storage = storage
    
    def set_storage(self, storage):
        logger.debug("Setting storage for NotificationService")
        self._storage = storage
        
    async def start(self):
        logger.info("Starting notification service loop")
        while True:
            try:
                await asyncio.sleep(0.1)
                if not self.notification_queue.empty():
                    notification_data = await self.notification_queue.get()
                    await self._process_notification(notification_data)
            except asyncio.CancelledError:
                logger.info("Notification service loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in notification service: {e}", exc_info=True)
    
    async def _process_notification(self, notification_data: dict):
        notification_type = notification_data.get("type")
        logger.debug(f"Processing notification type: {notification_type}")
        
        if notification_type == "new_window":
            await self._notify_new_window(notification_data["window"])
        elif notification_type == "window_closed":
            await self._notify_window_closed(notification_data["window_id"])
        elif notification_type == "reservation_cancelled":
            await self._notify_reservation_cancelled(
                notification_data["user_id"],
                notification_data["window_id"],
                notification_data["reason"]
            )
    
    async def notify_new_window(self, window: Window):
        logger.info(f"Queueing new window notification for window: {window.id}")
        await self.notification_queue.put({
            "type": "new_window",
            "window": window
        })
    
    async def notify_window_closed(self, window_id: str):
        logger.info(f"Queueing window closed notification for window: {window_id}")
        await self.notification_queue.put({
            "type": "window_closed",
            "window_id": window_id
        })
    
    async def notify_reservation_cancelled(self, user_id: str, window_id: str, reason: str):
        logger.info(f"Queueing reservation cancelled notification - User: {user_id}, Window: {window_id}, Reason: {reason}")
        await self.notification_queue.put({
            "type": "reservation_cancelled",
            "user_id": user_id,
            "window_id": window_id,
            "reason": reason
        })
    
    async def _notify_new_window(self, window: Window):
        logger.info(f"Processing new window notification for window: {window.id}")
        all_alerts = await self._storage.alerts.get_all()
        
        matched_users = 0
        for user_id, alerts in all_alerts.items():
            for alert in alerts:
                if self._matches_criteria(window, alert.criteria):
                    notification = Notification(
                        id=str(uuid.uuid4()),
                        user_id=user_id,
                        message=f"New window available: {window.satellite_name} ({window.satellite_type.value}) at {window.window_datetime}",
                        window_id=window.id
                    )
                    await self._storage.notifications.add(notification)
                    matched_users += 1
        logger.info(f"Sent new window notification to {matched_users} users")
    
    async def _notify_window_closed(self, window_id: str):
        logger.info(f"Processing window closed notification for window: {window_id}")
        reservations = await self._storage.reservations.get_by_window(window_id)
        window = await self._storage.windows.get(window_id)
        
        cancelled_count = 0
        for reservation in reservations:
            if reservation.status.value == "pending":
                notification = Notification(
                    id=str(uuid.uuid4()),
                    user_id=reservation.user_id,
                    message=f"Window {window.satellite_name} has closed. Your reservation has been cancelled.",
                    window_id=window_id
                )
                await self._storage.notifications.add(notification)
                cancelled_count += 1
        logger.info(f"Window closed - cancelled {cancelled_count} pending reservations")
    
    async def _notify_reservation_cancelled(self, user_id: str, window_id: str, reason: str):
        logger.info(f"Processing reservation cancelled notification - User: {user_id}, Reason: {reason}")
        window = await self._storage.windows.get(window_id)
        notification = Notification(
            id=str(uuid.uuid4()),
            user_id=user_id,
            message=f"Your reservation for {window.satellite_name} has been cancelled. Reason: {reason}",
            window_id=window_id
        )
        await self._storage.notifications.add(notification)
    
    def _matches_criteria(self, window: Window, criteria: AlertCriteria) -> bool:
        if criteria.satellite_types and window.satellite_type not in criteria.satellite_types:
            return False
        
        if criteria.start_date and window.window_datetime < criteria.start_date:
            return False
        
        if criteria.end_date and window.window_datetime > criteria.end_date:
            return False
        
        if criteria.location and window.location != criteria.location:
            return False
        
        return True

notification_service = NotificationService()