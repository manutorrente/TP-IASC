"""
Sharded Storage Layer

Extends base storage with shard-aware operations.
Routes operations to correct shards and handles replication.
"""

from typing import Dict, List, Optional, Any
from models import Operator, User, Window, Alert, Reservation, Notification
from cluster.shard_strategy import get_shard_strategy, EntityType
from cluster.shard_manager import get_shard_manager
from cluster.modification_history import modification_history
from fastapi import HTTPException
import asyncio
import logging

logger = logging.getLogger(__name__)


class ShardedRepository:
    """
    Base repository with shard awareness.
    Routes operations to correct shard based on entity ID.
    """
    
    def __init__(self, entity_type: str):
        self._data: Dict[int, Dict[str, Any]] = {}  # shard_id -> {entity_id -> entity}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._pending_writes: Dict[str, Any] = {}
        self._entity_type = entity_type
        
        # Initialize shard-specific storage
        self._initialize_shard_storage()
    
    def _initialize_shard_storage(self):
        """Initialize storage for each shard this node is responsible for"""
        try:
            shard_manager = get_shard_manager()
            my_shards = shard_manager.get_my_shards()
            
            for shard_id in my_shards:
                if shard_id not in self._data:
                    self._data[shard_id] = {}
                    logger.debug(f"{self._entity_type} repository: Initialized storage for shard {shard_id}")
        except RuntimeError:
            # ShardManager not initialized yet, will be initialized later
            logger.warning(f"{self._entity_type} repository: ShardManager not ready, deferring shard storage initialization")
    
    async def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create lock for entity"""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]
    
    def _get_shard_id(self, entity_id: str) -> int:
        """Calculate shard ID for entity"""
        shard_strategy = get_shard_strategy()
        return shard_strategy.calculate_shard_id(entity_id)
    
    def _get_shard_data(self, shard_id: int) -> Dict[str, Any]:
        """Get data dictionary for a shard"""
        if shard_id not in self._data:
            self._data[shard_id] = {}
        return self._data[shard_id]
    
    async def _replicate_and_commit(self, operation: str, entity_id: str, data: Any) -> bool:
        """
        Replicate write to shard replicas.
        
        Args:
            operation: Operation type (add, update, delete)
            entity_id: Entity identifier
            data: Entity data
            
        Returns:
            True if successful, False otherwise
        """
        shard_manager = get_shard_manager()
        shard_id = self._get_shard_id(entity_id)
        
        # Check if this node is master for the shard
        if not shard_manager.is_master_for_shard(shard_id):
            logger.error(f"Node is not master for shard {shard_id} (entity: {entity_id})")
            raise HTTPException(
                status_code=500, 
                detail=f"Node is not master for shard {shard_id}, cannot perform write operations"
            )
        
        # Convert data to dict
        data_dict = data.model_dump() if hasattr(data, 'model_dump') else data
        
        logger.debug(f"Replicating {operation} for {self._entity_type}:{entity_id} to shard {shard_id}")
        
        # Replicate to shard replicas
        success = await shard_manager.replicate_to_shard(
            shard_id, operation, self._entity_type, entity_id, data_dict
        )
        
        if not success:
            logger.warning(f"Shard replication failed for {operation} on {self._entity_type}:{entity_id}")
            return False
        
        logger.debug(f"Shard replication successful for {operation} on {self._entity_type}:{entity_id}")
        
        # Record in modification history
        await modification_history.record(self._entity_type, entity_id, operation, data)
        logger.debug(f"Recorded modification for {self._entity_type}:{entity_id} operation:{operation}")
        
        return True
    
    async def prepare_write(self, entity_id: str, data: Any):
        """Prepare write (2PC prepare phase)"""
        lock = await self._get_lock(entity_id)
        async with lock:
            self._pending_writes[entity_id] = data
            logger.debug(f"Prepared write for {self._entity_type}:{entity_id}")
    
    async def commit_write(self, entity_id: str):
        """Commit write (2PC commit phase)"""
        lock = await self._get_lock(entity_id)
        async with lock:
            if entity_id in self._pending_writes:
                shard_id = self._get_shard_id(entity_id)
                shard_data = self._get_shard_data(shard_id)
                shard_data[entity_id] = self._pending_writes[entity_id]
                del self._pending_writes[entity_id]
                logger.debug(f"Committed write for {self._entity_type}:{entity_id} to shard {shard_id}")
    
    async def abort_write(self, entity_id: str):
        """Abort write (2PC abort phase)"""
        lock = await self._get_lock(entity_id)
        async with lock:
            if entity_id in self._pending_writes:
                del self._pending_writes[entity_id]
                logger.debug(f"Aborted write for {self._entity_type}:{entity_id}")
    
    def _get_entity(self, entity_id: str) -> Optional[Any]:
        """Get entity from correct shard"""
        shard_id = self._get_shard_id(entity_id)
        shard_data = self._get_shard_data(shard_id)
        return shard_data.get(entity_id)
    
    def _set_entity(self, entity_id: str, entity: Any):
        """Set entity in correct shard"""
        shard_id = self._get_shard_id(entity_id)
        shard_data = self._get_shard_data(shard_id)
        shard_data[entity_id] = entity
    
    def _get_all_entities(self) -> List[Any]:
        """Get all entities from all shards this node has"""
        all_entities = []
        for shard_data in self._data.values():
            all_entities.extend(shard_data.values())
        return all_entities


class ShardedOperatorRepository(ShardedRepository):
    """Sharded operator repository"""
    
    def __init__(self):
        super().__init__("operator")
    
    async def add(self, operator: Operator) -> bool:
        lock = await self._get_lock(operator.id)
        async with lock:
            logger.info(f"Adding operator: {operator.id} - {operator.name}")
            if not await self._replicate_and_commit("add", operator.id, operator):
                logger.error(f"Failed to add operator: {operator.id}")
                return False
            self._set_entity(operator.id, operator)
            logger.info(f"Operator added successfully: {operator.id}")
            return True
    
    async def get(self, operator_id: str) -> Optional[Operator]:
        operator = self._get_entity(operator_id)
        logger.debug(f"Retrieved operator: {operator_id} - Found: {operator is not None}")
        return operator


class ShardedUserRepository(ShardedRepository):
    """Sharded user repository"""
    
    def __init__(self):
        super().__init__("user")
    
    async def add(self, user: User) -> bool:
        lock = await self._get_lock(user.id)
        async with lock:
            logger.info(f"Adding user: {user.id} - {user.name}")
            if not await self._replicate_and_commit("add", user.id, user):
                logger.error(f"Failed to add user: {user.id}")
                return False
            self._set_entity(user.id, user)
            logger.info(f"User added successfully: {user.id}")
            return True
    
    async def get(self, user_id: str) -> Optional[User]:
        user = self._get_entity(user_id)
        logger.debug(f"Retrieved user: {user_id} - Found: {user is not None}")
        return user


class ShardedWindowRepository(ShardedRepository):
    """Sharded window repository"""
    
    def __init__(self):
        super().__init__("window")
    
    async def add(self, window: Window) -> bool:
        lock = await self._get_lock(window.id)
        async with lock:
            logger.info(f"Adding window: {window.id} - {window.satellite_name}")
            if not await self._replicate_and_commit("add", window.id, window):
                logger.error(f"Failed to add window: {window.id}")
                return False
            self._set_entity(window.id, window)
            logger.info(f"Window added successfully: {window.id}")
            return True
    
    async def get(self, window_id: str) -> Optional[Window]:
        window = self._get_entity(window_id)
        logger.debug(f"Retrieved window: {window_id} - Found: {window is not None}")
        return window
    
    async def update(self, window: Window) -> bool:
        lock = await self._get_lock(window.id)
        async with lock:
            logger.info(f"Updating window: {window.id} - Status: {window.status}")
            if not await self._replicate_and_commit("update", window.id, window):
                logger.error(f"Failed to update window: {window.id}")
                return False
            self._set_entity(window.id, window)
            logger.info(f"Window updated successfully: {window.id}")
            return True
    
    async def get_all(self) -> List[Window]:
        """Get all windows from shards this node has"""
        windows = self._get_all_entities()
        logger.debug(f"Retrieving all windows - Count: {len(windows)}")
        return windows


class ShardedReservationRepository(ShardedRepository):
    """Sharded reservation repository"""
    
    def __init__(self):
        super().__init__("reservation")
        self._window_index: Dict[int, Dict[str, List[str]]] = {}  # shard_id -> {window_id -> [reservation_ids]}
        self._user_index: Dict[int, Dict[str, List[str]]] = {}  # shard_id -> {user_id -> [reservation_ids]}
    
    async def add(self, reservation: Reservation) -> bool:
        lock = await self._get_lock(reservation.id)
        async with lock:
            logger.info(f"Adding reservation: {reservation.id}")
            if not await self._replicate_and_commit("add", reservation.id, reservation):
                logger.error(f"Failed to add reservation: {reservation.id}")
                return False
            
            self._set_entity(reservation.id, reservation)
            
            # Update indices
            shard_id = self._get_shard_id(reservation.id)
            if shard_id not in self._window_index:
                self._window_index[shard_id] = {}
            if shard_id not in self._user_index:
                self._user_index[shard_id] = {}
            
            if reservation.window_id not in self._window_index[shard_id]:
                self._window_index[shard_id][reservation.window_id] = []
            self._window_index[shard_id][reservation.window_id].append(reservation.id)
            
            if reservation.user_id not in self._user_index[shard_id]:
                self._user_index[shard_id][reservation.user_id] = []
            self._user_index[shard_id][reservation.user_id].append(reservation.id)
            
            logger.info(f"Reservation added successfully: {reservation.id}")
            return True
    
    async def get(self, reservation_id: str) -> Optional[Reservation]:
        reservation = self._get_entity(reservation_id)
        logger.debug(f"Retrieved reservation: {reservation_id} - Found: {reservation is not None}")
        return reservation
    
    async def update(self, reservation: Reservation) -> bool:
        lock = await self._get_lock(reservation.id)
        async with lock:
            logger.info(f"Updating reservation: {reservation.id}")
            if not await self._replicate_and_commit("update", reservation.id, reservation):
                logger.error(f"Failed to update reservation: {reservation.id}")
                return False
            self._set_entity(reservation.id, reservation)
            logger.info(f"Reservation updated successfully: {reservation.id}")
            return True
    
    async def get_by_window(self, window_id: str) -> List[Reservation]:
        """Get reservations by window - may span multiple shards"""
        reservations = []
        for shard_id, window_index in self._window_index.items():
            reservation_ids = window_index.get(window_id, [])
            shard_data = self._get_shard_data(shard_id)
            reservations.extend([shard_data[rid] for rid in reservation_ids if rid in shard_data])
        logger.debug(f"Retrieved reservations for window {window_id} - Count: {len(reservations)}")
        return reservations
    
    async def get_by_user(self, user_id: str) -> List[Reservation]:
        """Get reservations by user - may span multiple shards"""
        reservations = []
        for shard_id, user_index in self._user_index.items():
            reservation_ids = user_index.get(user_id, [])
            shard_data = self._get_shard_data(shard_id)
            reservations.extend([shard_data[rid] for rid in reservation_ids if rid in shard_data])
        logger.debug(f"Retrieved reservations for user {user_id} - Count: {len(reservations)}")
        return reservations


class ShardedAlertRepository(ShardedRepository):
    """Sharded alert repository - sharded by user_id"""
    
    def __init__(self):
        super().__init__("alert")
    
    async def add(self, user_id: str, alert: Alert) -> bool:
        # Shard by user_id
        lock = await self._get_lock(user_id)
        async with lock:
            logger.info(f"Adding alert: {alert.id} for user: {user_id}")
            
            if not await self._replicate_and_commit("add", alert.id, alert):
                logger.error(f"Failed to add alert: {alert.id}")
                return False
            
            shard_id = self._get_shard_id(user_id)
            shard_data = self._get_shard_data(shard_id)
            
            if user_id not in shard_data:
                shard_data[user_id] = []
            shard_data[user_id].append(alert)
            
            logger.info(f"Alert added successfully: {alert.id}")
            return True
    
    async def get_by_user(self, user_id: str) -> List[Alert]:
        shard_id = self._get_shard_id(user_id)
        shard_data = self._get_shard_data(shard_id)
        alerts = shard_data.get(user_id, []).copy()
        logger.debug(f"Retrieved alerts for user {user_id} - Count: {len(alerts)}")
        return alerts
    
    async def get_all(self) -> Dict[str, List[Alert]]:
        """Get all alerts from all shards"""
        all_alerts = {}
        for shard_data in self._data.values():
            for user_id, alerts in shard_data.items():
                if user_id not in all_alerts:
                    all_alerts[user_id] = []
                all_alerts[user_id].extend(alerts.copy())
        logger.debug(f"Retrieved all alerts - User count: {len(all_alerts)}")
        return all_alerts


class ShardedNotificationRepository(ShardedRepository):
    """Sharded notification repository - sharded by user_id"""
    
    def __init__(self):
        super().__init__("notification")
    
    async def add(self, notification: Notification) -> bool:
        # Shard by user_id
        lock = await self._get_lock(notification.user_id)
        async with lock:
            logger.info(f"Adding notification: {notification.id} for user: {notification.user_id}")
            
            if not await self._replicate_and_commit("add", notification.id, notification):
                logger.error(f"Failed to add notification: {notification.id}")
                return False
            
            shard_id = self._get_shard_id(notification.user_id)
            shard_data = self._get_shard_data(shard_id)
            
            if notification.user_id not in shard_data:
                shard_data[notification.user_id] = []
            shard_data[notification.user_id].append(notification)
            
            logger.info(f"Notification added successfully: {notification.id}")
            return True
    
    async def get_by_user(self, user_id: str) -> List[Notification]:
        shard_id = self._get_shard_id(user_id)
        shard_data = self._get_shard_data(shard_id)
        notifications = shard_data.get(user_id, []).copy()
        logger.debug(f"Retrieved notifications for user {user_id} - Count: {len(notifications)}")
        return notifications


class ShardedStorage:
    """Main storage interface with sharding support"""
    
    def __init__(self):
        self.operators = ShardedOperatorRepository()
        self.users = ShardedUserRepository()
        self.windows = ShardedWindowRepository()
        self.alerts = ShardedAlertRepository()
        self.reservations = ShardedReservationRepository()
        self.notifications = ShardedNotificationRepository()
    
    def get_repository(self, entity_type: str) -> Optional[ShardedRepository]:
        """Get repository by entity type"""
        repo_map = {
            "operator": self.operators,
            "user": self.users,
            "window": self.windows,
            "alert": self.alerts,
            "reservation": self.reservations,
            "notification": self.notifications
        }
        return repo_map.get(entity_type)


# Global sharded storage instance
sharded_storage = ShardedStorage()
