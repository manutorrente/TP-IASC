from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List
from datetime import datetime
from dependencies import get_storage, get_cluster_manager
from cluster.cluster_manager import ClusterManager
from storage import Storage
from models import Operator, User, Window, Alert, Reservation, Notification
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

# Mapping of entity types to their Pydantic models
ENTITY_MODEL_MAP = {
    "operator": Operator,
    "user": User,
    "window": Window,
    "alert": Alert,
    "reservation": Reservation,
    "notification": Notification,
}

class PrepareRequest(BaseModel):
    operation: str
    entity_type: str
    entity_id: str
    data: Dict[str, Any]

class CommitRequest(BaseModel):
    entity_type: str
    entity_id: str
    shard_id: int

class AbortRequest(BaseModel):
    entity_type: str
    entity_id: str

class ModificationItem(BaseModel):
    operation: str
    entity_type: str
    entity_id: str
    data: Dict[str, Any]
    timestamp: str

class BatchCatchupRequest(BaseModel):
    modifications: List[ModificationItem]

@router.post("/prepare")
async def prepare_write(request: PrepareRequest, storage: Storage = Depends(get_storage)):
    try:
        logger.info(f"Preparing write - Operation: {request.operation}, Entity: {request.entity_type}:{request.entity_id}")
        repository = storage.get_repository(request.entity_type)
        if not repository:
            logger.warning(f"Invalid entity type: {request.entity_type}")
            raise HTTPException(status_code=400, detail="Invalid entity type")
        
        # Deserialize the data dict to the appropriate Pydantic model
        model_class = ENTITY_MODEL_MAP.get(request.entity_type)
        if model_class:
            try:
                deserialized_data = model_class(**request.data)
            except Exception as e:
                logger.error(f"Error deserializing data for {request.entity_type}: {e}", exc_info=True)
                raise HTTPException(status_code=400, detail=f"Invalid data format: {str(e)}")
        else:
            deserialized_data = request.data
        
        await repository.prepare_write(request.entity_id, deserialized_data)
        logger.info(f"Write prepared successfully for {request.entity_type}:{request.entity_id}")
        return {"status": "prepared"}
    except Exception as e:
        logger.error(f"Error preparing write: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/commit")
async def commit_write(request: CommitRequest, storage: Storage = Depends(get_storage), cluster_manager = Depends(get_cluster_manager)):
    try:
        logger.info(f"Committing write - Entity: {request.entity_type}:{request.entity_id}, Shard: {request.shard_id}")
        repository = storage.get_repository(request.entity_type)
        if not repository:
            logger.warning(f"Invalid entity type: {request.entity_type}")
            raise HTTPException(status_code=400, detail="Invalid entity type")
        
        await repository.commit_write(request.entity_id)
        
        # Update the last_update timestamp for the slave shard that received this update
        for shard in cluster_manager.shards:
            if shard.shard_id == request.shard_id and shard.role.value == "replica":
                from datetime import datetime
                shard.last_update = datetime.utcnow()
                logger.info(f"Updated last_update timestamp for slave shard {request.shard_id}")
                break
        
        logger.info(f"Write committed successfully for {request.entity_type}:{request.entity_id}")
        return {"status": "committed"}
    except Exception as e:
        logger.error(f"Error committing write: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/abort")
async def abort_write(request: AbortRequest, storage: Storage = Depends(get_storage)):
    try:
        logger.info(f"Aborting write - Entity: {request.entity_type}:{request.entity_id}")
        repository = storage.get_repository(request.entity_type)
        if not repository:
            logger.warning(f"Invalid entity type: {request.entity_type}")
            raise HTTPException(status_code=400, detail="Invalid entity type")
        
        await repository.abort_write(request.entity_id)
        logger.info(f"Write aborted successfully for {request.entity_type}:{request.entity_id}")
        return {"status": "aborted"}
    except Exception as e:
        logger.error(f"Error aborting write: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/batch-catchup")
async def batch_catchup(request: BatchCatchupRequest, storage: Storage = Depends(get_storage)):
    """Receive batch of modifications to catch up this node."""
    try:
        logger.info(f"Received batch catchup request with {len(request.modifications)} modifications")
        
        applied_count = 0
        failed_count = 0
        
        for mod in request.modifications:
            try:
                repository = storage.get_repository(mod.entity_type)
                if not repository:
                    logger.warning(f"Invalid entity type during catchup: {mod.entity_type}")
                    failed_count += 1
                    continue
                
                logger.debug(f"Applying catchup modification: {mod.entity_type}:{mod.entity_id} operation:{mod.operation}")
                
                # Deserialize the data dict to the appropriate Pydantic model
                model_class = ENTITY_MODEL_MAP.get(mod.entity_type)
                if model_class:
                    try:
                        deserialized_data = model_class(**mod.data)
                    except Exception as e:
                        logger.error(f"Error deserializing data for {mod.entity_type}: {e}", exc_info=True)
                        failed_count += 1
                        continue
                else:
                    deserialized_data = mod.data
                
                await repository.prepare_write(mod.entity_id, deserialized_data)
                await repository.commit_write(mod.entity_id)
                
                applied_count += 1
                
            except Exception as e:
                logger.error(f"Error applying modification {mod.entity_type}:{mod.entity_id}: {e}", exc_info=True)
                failed_count += 1
        
        logger.info(f"Batch catchup completed - Applied: {applied_count}, Failed: {failed_count}")
        
        return {
            "status": "completed",
            "applied": applied_count,
            "failed": failed_count,
            "total": len(request.modifications)
        }
        
    except Exception as e:
        logger.error(f"Error processing batch catchup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/status")
async def cluster_status(cluster_manager: ClusterManager = Depends(get_cluster_manager)):
    try:
        logger.info("Fetching cluster status")
        status = cluster_manager.get_status()
        logger.info("Cluster status fetched successfully")
        return status
    except Exception as e:
        logger.error(f"Error fetching cluster status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class ShardUpdateRequest(BaseModel):
    shard_state: Dict[str, Dict[str, List[int]]]


@router.post("/shard-update")
async def shard_update(
    request: ShardUpdateRequest,
    cluster_manager: ClusterManager = Depends(get_cluster_manager)
):
    """
    Update local shard state when another instance notifies changes.
    """
    try:
        logger.info("Received shard update notification")

        cluster_manager.update_shard_state(request.shard_state)

        logger.info("Shard state updated successfully")
        return {"status": "ok"}

    except Exception as e:
        logger.error(f"Error handling shard update: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shard/{shard_id}/last-update")
async def get_shard_last_update(
    shard_id: int,
    cluster_manager: ClusterManager = Depends(get_cluster_manager)
):
    """
    Get the last update timestamp for a specific shard on this node.
    
    Args:
        shard_id: The shard ID to query
    
    Returns:
        {
            "shard_id": int,
            "last_update": ISO8601 datetime string or null,
            "role": "master" or "replica",
            "found": bool
        }
    """
    try:
        logger.debug(f"Querying last update for shard {shard_id}")
        
        # Search for the shard in this node's shards
        for shard in cluster_manager.shards:
            if shard.shard_id == shard_id:
                logger.info(f"Found shard {shard_id} with role {shard.role.value}, last_update: {shard.last_update}")
                return {
                    "shard_id": shard.shard_id,
                    "last_update": shard.last_update.isoformat() if shard.last_update else None,
                    "role": shard.role.value,
                    "found": True
                }
        
        logger.warning(f"Shard {shard_id} not found on this node")
        return {
            "shard_id": shard_id,
            "last_update": None,
            "role": None,
            "found": False
        }
    
    except Exception as e:
        logger.error(f"Error querying shard last update: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))