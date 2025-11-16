import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from collections import OrderedDict
from config import settings
import logging

logger = logging.getLogger(__name__)

class ModificationRecord:
    def __init__(self, entity_type: str, entity_id: str, operation: str, 
                 data: Any, timestamp: datetime):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.operation = operation
        self.data = data
        self.timestamp = timestamp

class ModificationHistory:
    def __init__(self):
        logger.debug("Initializing ModificationHistory")
        self._history: OrderedDict[str, ModificationRecord] = OrderedDict()
        self._lock = asyncio.Lock()
    
    def _make_key(self, entity_type: str, entity_id: str) -> str:
        return f"{entity_type}:{entity_id}"
    
    async def record(self, entity_type: str, entity_id: str, operation: str, data: Any):
        async with self._lock:
            key = self._make_key(entity_type, entity_id)
            record = ModificationRecord(
                entity_type=entity_type,
                entity_id=entity_id,
                operation=operation,
                data=data,
                timestamp=datetime.utcnow()
            )
            
            if key in self._history:
                del self._history[key]
            
            self._history[key] = record
            self._history.move_to_end(key)
            logger.debug(f"Recorded modification: {entity_type}:{entity_id} operation:{operation}")
    
    async def get_record(self, entity_type: str, entity_id: str) -> Optional[ModificationRecord]:
        async with self._lock:
            key = self._make_key(entity_type, entity_id)
            record = self._history.get(key)
            logger.debug(f"Retrieved record: {key} - Found: {record is not None}")
            return record
    
    async def get_recent_modifications(self, since: datetime | None) -> list[ModificationRecord]:
        async with self._lock:
            recent = [
                record for record in self._history.values()
                if since is None or record.timestamp >= since
            ]
            logger.debug(f"Retrieved {len(recent)} modifications since {since}")
            return recent
    
    async def cleanup_old_records(self):
        cutoff_time = datetime.utcnow() - timedelta(seconds=settings.modification_history_ttl_seconds)
        
        async with self._lock:
            keys_to_remove = [
                key for key, record in self._history.items()
                if record.timestamp < cutoff_time
            ]
            
            for key in keys_to_remove:
                del self._history[key]
            
            if keys_to_remove:
                logger.info(f"Cleaned up {len(keys_to_remove)} old modification records")
    
    async def start_cleanup_loop(self):
        logger.info("Starting modification history cleanup loop")
        while True:
            try:
                await asyncio.sleep(300)
                await self.cleanup_old_records()
            except asyncio.CancelledError:
                logger.info("Modification history cleanup loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}", exc_info=True)

modification_history = ModificationHistory()