from storage import Storage, storage
from services.notification_service import NotificationService, notification_service
from services.window_service import WindowService, window_service
from services.reservation_service import ReservationService, reservation_service
from cluster.cluster_manager import cluster_manager, ClusterManager
from cluster.modification_history import modification_history, ModificationHistory

def get_storage() -> Storage:
    return storage

def get_notification_service() -> NotificationService:
    return notification_service

def get_window_service() -> WindowService:
    return window_service

def get_reservation_service() -> ReservationService:
    return reservation_service

def get_cluster_manager() -> ClusterManager:
    return cluster_manager

def get_modification_history() -> ModificationHistory:
    return modification_history