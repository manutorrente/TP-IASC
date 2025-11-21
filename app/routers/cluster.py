from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
from datetime import datetime
from dependencies import get_storage, get_cluster_manager
from cluster.cluster_manager import ClusterManager
from storage import Storage
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

# Import shard manager (will be available after initialization)
try:
    from cluster.shard_manager import get_shard_manager
    SHARDING_ENABLED = True
except Exception:
    SHARDING_ENABLED = False
    logger.warning("Sharding not enabled")

class PrepareRequest(BaseModel):
    operation: str
    entity_type: str
    entity_id: str
    data: Dict[str, Any]

class CommitRequest(BaseModel):
    entity_type: str
    entity_id: str

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
        
        await repository.prepare_write(request.entity_id, request.data)
        logger.info(f"Write prepared successfully for {request.entity_type}:{request.entity_id}")
        return {"status": "prepared"}
    except Exception as e:
        logger.error(f"Error preparing write: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/commit")
async def commit_write(request: CommitRequest, storage: Storage = Depends(get_storage)):
    try:
        logger.info(f"Committing write - Entity: {request.entity_type}:{request.entity_id}")
        repository = storage.get_repository(request.entity_type)
        if not repository:
            logger.warning(f"Invalid entity type: {request.entity_type}")
            raise HTTPException(status_code=400, detail="Invalid entity type")
        
        await repository.commit_write(request.entity_id)
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
                
                await repository.prepare_write(mod.entity_id, mod.data)
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
        
        # Add shard status if sharding is enabled
        if SHARDING_ENABLED:
            try:
                shard_manager = get_shard_manager()
                status["sharding"] = shard_manager.get_status()
            except Exception as e:
                logger.warning(f"Could not get shard status: {e}")
                status["sharding"] = {"error": "not initialized"}
        
        logger.info("Cluster status fetched successfully")
        return status
    except Exception as e:
        logger.error(f"Error fetching cluster status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Shard-specific endpoints
class ShardPrepareRequest(BaseModel):
    shard_id: int
    operation: str
    entity_type: str
    entity_id: str
    data: Dict[str, Any]


class ShardCommitRequest(BaseModel):
    shard_id: int
    entity_type: str
    entity_id: str


class ShardAbortRequest(BaseModel):
    shard_id: int
    entity_type: str
    entity_id: str


class NewShardMasterRequest(BaseModel):
    shard_id: int
    master_url: str


@router.post("/shard-prepare")
async def shard_prepare(request: ShardPrepareRequest, storage: Storage = Depends(get_storage)):
    """Prepare write for a specific shard (2PC prepare phase)"""
    try:
        logger.info(f"Shard {request.shard_id} prepare - {request.operation} {request.entity_type}:{request.entity_id}")
        
        repository = storage.get_repository(request.entity_type)
        if not repository:
            raise HTTPException(status_code=400, detail="Invalid entity type")
        
        await repository.prepare_write(request.entity_id, request.data)
        logger.info(f"Shard {request.shard_id} prepare successful")
        return {"status": "prepared", "shard_id": request.shard_id}
    except Exception as e:
        logger.error(f"Shard prepare error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shard-commit")
async def shard_commit(request: ShardCommitRequest, storage: Storage = Depends(get_storage)):
    """Commit write for a specific shard (2PC commit phase)"""
    try:
        logger.info(f"Shard {request.shard_id} commit - {request.entity_type}:{request.entity_id}")
        
        repository = storage.get_repository(request.entity_type)
        if not repository:
            raise HTTPException(status_code=400, detail="Invalid entity type")
        
        await repository.commit_write(request.entity_id)
        logger.info(f"Shard {request.shard_id} commit successful")
        return {"status": "committed", "shard_id": request.shard_id}
    except Exception as e:
        logger.error(f"Shard commit error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shard-abort")
async def shard_abort(request: ShardAbortRequest, storage: Storage = Depends(get_storage)):
    """Abort write for a specific shard (2PC abort phase)"""
    try:
        logger.info(f"Shard {request.shard_id} abort - {request.entity_type}:{request.entity_id}")
        
        repository = storage.get_repository(request.entity_type)
        if not repository:
            raise HTTPException(status_code=400, detail="Invalid entity type")
        
        await repository.abort_write(request.entity_id)
        logger.info(f"Shard {request.shard_id} abort successful")
        return {"status": "aborted", "shard_id": request.shard_id}
    except Exception as e:
        logger.error(f"Shard abort error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/new-shard-master")
async def new_shard_master(request: NewShardMasterRequest):
    """Notification of new shard master election"""
    try:
        logger.info(f"New master for shard {request.shard_id}: {request.master_url}")
        
        if SHARDING_ENABLED:
            shard_manager = get_shard_manager()
            # Update local shard manager state
            # This would update the master for the shard
            logger.info(f"Updated shard {request.shard_id} master to {request.master_url}")
        
        return {"status": "acknowledged", "shard_id": request.shard_id}
    except Exception as e:
        logger.error(f"Error updating shard master: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))