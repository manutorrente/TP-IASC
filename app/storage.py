from typing import Dict, List, Optional, Any
from models import Operator, User, Window, Alert, Reservation, Notification
from cluster.cluster_manager import cluster_manager
from cluster.modification_history import modification_history
from fastapi import HTTPException
import asyncio
import logging
import httpx


logger = logging.getLogger(__name__)

class ReplicationError(Exception):
    pass

class PendingWrite:
    def __init__(self, data: Any):
        self.data = data
        self.committed = False

class BaseRepository:
    def __init__(self, entity_type: str):
        self._data: Dict = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._pending_writes: Dict[str, PendingWrite] = {}
        self._entity_type = entity_type
    
    async def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]
    
    async def _replicate_and_commit(self, operation: str, entity_id: str, data: Any) -> bool:
        try:
            logger.debug(f"Starting replication: operation={operation}, entity_type={self._entity_type}, entity_id={entity_id}")
            
            if not cluster_manager._is_master_for_entity(entity_id):
                logger.info(f"Not master node for {self._entity_type}:{entity_id}, finding master and forwarding request")
                
                # Try to find and forward to master
                shard_id = cluster_manager._get_shard_for_entity(entity_id)
                master_node = None
                
                # Check if any cluster node is master of this shard
                for node in cluster_manager.cluster_nodes:
                    if node.is_master_of(shard_id):
                        master_node = node
                        break
                
                if not master_node:
                    logger.error(f"Could not find master node for shard {shard_id} containing {self._entity_type}:{entity_id}")
                    raise HTTPException(status_code=503, detail=f"Master node not found for {self._entity_type}:{entity_id}")
                
                logger.debug(f"Found master at {master_node.url}, forwarding write request")
                
                # Convert data to dict for forwarding
                data_dict = data.model_dump(mode='json') if hasattr(data, 'model_dump') else data
                
                # Forward to master node via HTTP
                async with httpx.AsyncClient(timeout=5.0) as client:
                    try:
                        logger.debug(f"Sending forward-write request to {master_node.url} for {self._entity_type}:{entity_id}")
                        response = await client.post(
                            f"{master_node.url}/cluster/forward-write",
                            json={
                                "operation": operation,
                                "entity_type": self._entity_type,
                                "entity_id": entity_id,
                                "data": data_dict
                            }
                        )
                        
                        logger.debug(f"Received response from master: status={response.status_code}")
                        
                        if response.status_code == 200:
                            logger.info(f"Write request forwarded successfully to {master_node.url} for {self._entity_type}:{entity_id}")
                            return True
                        else:
                            logger.error(f"Master node at {master_node.url} returned error status: {response.status_code}")
                            try:
                                error_detail = response.json()
                                logger.error(f"Master error response body: {error_detail}")
                            except Exception as parse_e:
                                logger.error(f"Master error response text: {response.text}")
                            return False
                    except httpx.TimeoutException as te:
                        logger.error(f"Timeout forwarding write to master at {master_node.url}: {te}", exc_info=True)
                        return False
                    except httpx.ConnectError as ce:
                        logger.error(f"Connection error forwarding write to master at {master_node.url}: {ce}", exc_info=True)
                        return False
                    except Exception as e:
                        logger.error(f"Error forwarding write to master at {master_node.url}: {type(e).__name__}: {e}", exc_info=True)
                        return False
            
            logger.debug(f"Node is master, converting data to dict")
            data_dict = data.model_dump(mode='json') if hasattr(data, 'model_dump') else data
            logger.debug(f"Data converted successfully: {type(data_dict)}")
            
            logger.debug(f"Calling cluster_manager.replicate_write for {self._entity_type}:{entity_id}")
            success = await cluster_manager.replicate_write(
                operation, self._entity_type, entity_id, data_dict
            )
            
            if not success:
                logger.error(f"Replication failed for {operation} on {self._entity_type}:{entity_id}")
                return False
            
            logger.debug(f"Replication successful for {operation} on {self._entity_type}:{entity_id}")
            
            logger.debug(f"Recording modification history for {self._entity_type}:{entity_id}")
            await modification_history.record(self._entity_type, entity_id, operation, data)
            logger.debug(f"Recorded modification for {self._entity_type}:{entity_id} operation:{operation}")
            return True
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Exception in _replicate_and_commit for {self._entity_type}:{entity_id}: {str(e)}", exc_info=True)
            return False
    
    async def prepare_write(self, entity_id: str, data: Any):
        lock = await self._get_lock(entity_id)
        async with lock:
            self._pending_writes[entity_id] = PendingWrite(data)
    
    async def commit_write(self, entity_id: str):
        lock = await self._get_lock(entity_id)
        async with lock:
            if entity_id in self._pending_writes:
                pending = self._pending_writes[entity_id]
                pending.committed = True
                self._data[entity_id] = pending.data
                del self._pending_writes[entity_id]
    
    async def abort_write(self, entity_id: str):
        lock = await self._get_lock(entity_id)
        async with lock:
            if entity_id in self._pending_writes:
                del self._pending_writes[entity_id]

class OperatorRepository(BaseRepository):
    def __init__(self):
        super().__init__("operator")
        self._data: Dict[str, Operator] = {}
    
    async def add(self, operator: Operator) -> bool:
        lock = await self._get_lock(operator.id)
        async with lock:
            logger.info(f"Adding operator: {operator.id} - {operator.name}")
            if not await self._replicate_and_commit("add", operator.id, operator):
                logger.error(f"Failed to add operator: {operator.id}")
                return False
            self._data[operator.id] = operator
            logger.info(f"Operator added successfully: {operator.id}")
            return True
    
    async def get(self, operator_id: str) -> Optional[Operator]:
        operator = self._data.get(operator_id)
        logger.debug(f"Retrieved operator: {operator_id} - Found: {operator is not None}")
        return operator

class UserRepository(BaseRepository):
    def __init__(self):
        super().__init__("user")
        self._data: Dict[str, User] = {}
    
    async def add(self, user: User) -> bool:
        try:
            lock = await self._get_lock(user.id)
            async with lock:
                logger.info(f"Adding user: {user.id} - {user.name}")
                logger.debug(f"Acquired lock for user {user.id}")
                if not await self._replicate_and_commit("add", user.id, user):
                    logger.error(f"Failed to add user: {user.id} - replication failed")
                    return False
                self._data[user.id] = user
                logger.info(f"User added successfully: {user.id}")
                return True
        except Exception as e:
            logger.error(f"Exception in UserRepository.add for {user.id}: {str(e)}", exc_info=True)
            raise
    
    async def get(self, user_id: str) -> Optional[User]:
        user = self._data.get(user_id)
        logger.debug(f"Retrieved user: {user_id} - Found: {user is not None}")
        return user

class WindowRepository(BaseRepository):
    def __init__(self):
        super().__init__("window")
        self._data: Dict[str, Window] = {}
    
    async def add(self, window: Window) -> bool:
        lock = await self._get_lock(window.id)
        async with lock:
            logger.info(f"Adding window: {window.id} - {window.satellite_name} by operator {window.operator_id}")
            if not await self._replicate_and_commit("add", window.id, window):
                logger.error(f"Failed to add window: {window.id}")
                return False
            self._data[window.id] = window
            logger.info(f"Window added successfully: {window.id}")
            return True
    
    async def get(self, window_id: str) -> Optional[Window]:
        window = self._data.get(window_id)
        logger.debug(f"Retrieved window: {window_id} - Found: {window is not None}")
        return window
    
    async def update(self, window: Window) -> bool:
        lock = await self._get_lock(window.id)
        async with lock:
            logger.info(f"Updating window: {window.id} - Status: {window.status}")
            if not await self._replicate_and_commit("update", window.id, window):
                logger.error(f"Failed to update window: {window.id}")
                return False
            self._data[window.id] = window
            logger.info(f"Window updated successfully: {window.id}")
            return True
    
    async def get_all(self) -> List[Window]:
        logger.debug(f"Retrieving all windows - Count: {len(self._data)}")
        return list(self._data.values())

class AlertRepository(BaseRepository):
    def __init__(self):
        super().__init__("alert")
        self._data: Dict[str, List[Alert]] = {}
    
    async def add(self, user_id: str, alert: Alert) -> bool:
        lock = await self._get_lock(user_id)
        async with lock:
            logger.info(f"Adding alert: {alert.id} for user: {user_id}")
            if user_id not in self._data:
                self._data[user_id] = []
            
            if not await self._replicate_and_commit("add", alert.id, alert):
                logger.error(f"Failed to add alert: {alert.id} for user: {user_id}")
                return False
            
            self._data[user_id].append(alert)
            logger.info(f"Alert added successfully: {alert.id}")
            return True
    
    async def get_by_user(self, user_id: str) -> List[Alert]:
        alerts = self._data.get(user_id, []).copy()
        logger.debug(f"Retrieved alerts for user {user_id} - Count: {len(alerts)}")
        return alerts
    
    async def get_all(self) -> Dict[str, List[Alert]]:
        logger.debug(f"Retrieved all alerts - User count: {len(self._data)}")
        return {k: v.copy() for k, v in self._data.items()}

class ReservationRepository(BaseRepository):
    def __init__(self):
        super().__init__("reservation")
        self._data: Dict[str, Reservation] = {}
        self._window_index: Dict[str, List[str]] = {}
        self._user_index: Dict[str, List[str]] = {}
    
    async def add(self, reservation: Reservation) -> bool:
        lock = await self._get_lock(reservation.id)
        async with lock:
            logger.info(f"Adding reservation: {reservation.id} - Window: {reservation.window_id}, User: {reservation.user_id}")
            if not await self._replicate_and_commit("add", reservation.id, reservation):
                logger.error(f"Failed to add reservation: {reservation.id}")
                return False
            
            self._data[reservation.id] = reservation
            
            if reservation.window_id not in self._window_index:
                self._window_index[reservation.window_id] = []
            self._window_index[reservation.window_id].append(reservation.id)
            
            if reservation.user_id not in self._user_index:
                self._user_index[reservation.user_id] = []
            self._user_index[reservation.user_id].append(reservation.id)
            logger.info(f"Reservation added successfully: {reservation.id}")
            return True
    
    async def get(self, reservation_id: str) -> Optional[Reservation]:
        reservation = self._data.get(reservation_id)
        logger.debug(f"Retrieved reservation: {reservation_id} - Found: {reservation is not None}")
        return reservation
    
    async def update(self, reservation: Reservation) -> bool:
        lock = await self._get_lock(reservation.id)
        async with lock:
            logger.info(f"Updating reservation: {reservation.id} - Status: {reservation.status}")
            if not await self._replicate_and_commit("update", reservation.id, reservation):
                logger.error(f"Failed to update reservation: {reservation.id}")
                return False
            self._data[reservation.id] = reservation
            logger.info(f"Reservation updated successfully: {reservation.id}")
            return True
    
    async def get_by_window(self, window_id: str) -> List[Reservation]:
        reservation_ids = self._window_index.get(window_id, [])
        reservations = [self._data[rid] for rid in reservation_ids if rid in self._data]
        logger.debug(f"Retrieved reservations for window {window_id} - Count: {len(reservations)}")
        return reservations
    
    async def get_by_user(self, user_id: str) -> List[Reservation]:
        reservation_ids = self._user_index.get(user_id, [])
        reservations = [self._data[rid] for rid in reservation_ids if rid in self._data]
        logger.debug(f"Retrieved reservations for user {user_id} - Count: {len(reservations)}")
        return reservations

class NotificationRepository(BaseRepository):
    def __init__(self):
        super().__init__("notification")
        self._data: Dict[str, List[Notification]] = {}
    
    async def add(self, notification: Notification) -> bool:
        lock = await self._get_lock(notification.user_id)
        async with lock:
            logger.info(f"Adding notification: {notification.id} for user: {notification.user_id}")
            if notification.user_id not in self._data:
                self._data[notification.user_id] = []
            
            if not await self._replicate_and_commit("add", notification.id, notification):
                logger.error(f"Failed to add notification: {notification.id}")
                return False
            
            self._data[notification.user_id].append(notification)
            logger.info(f"Notification added successfully: {notification.id}")
            return True
    
    async def get_by_user(self, user_id: str) -> List[Notification]:
        notifications = self._data.get(user_id, []).copy()
        logger.debug(f"Retrieved notifications for user {user_id} - Count: {len(notifications)}")
        return notifications

class Storage:
    def __init__(self):
        self.operators = OperatorRepository()
        self.users = UserRepository()
        self.windows = WindowRepository()
        self.alerts = AlertRepository()
        self.reservations = ReservationRepository()
        self.notifications = NotificationRepository()
    
    def get_repository(self, entity_type: str) -> Optional[BaseRepository]:
        repo_map = {
            "operator": self.operators,
            "user": self.users,
            "window": self.windows,
            "alert": self.alerts,
            "reservation": self.reservations,
            "notification": self.notifications
        }
        return repo_map.get(entity_type)
    

storage = Storage()