from fastapi import APIRouter, HTTPException, Depends
from models import Window, Resource
from storage import Storage
from services.window_service import WindowService
from dependencies import get_storage, get_window_service
from typing import List
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("", response_model=List[Window])
async def get_all_windows(storage: Storage = Depends(get_storage)):
    logger.info("Fetching all windows")
    windows = await storage.windows.get_all()
    logger.info(f"Retrieved {len(windows)} windows")
    return windows

@router.get("/{window_id}", response_model=Window)
async def get_window(window_id: str, storage: Storage = Depends(get_storage)):
    logger.info(f"Fetching window: {window_id}")
    window = await storage.windows.get(window_id)
    if not window:
        logger.warning(f"Window not found: {window_id}")
        raise HTTPException(status_code=404, detail="Window not found")
    return window

@router.get("/{window_id}/resources", response_model=List[Resource])
async def get_window_resources(window_id: str, storage: Storage = Depends(get_storage)):
    logger.info(f"Fetching resources for window: {window_id}")
    window = await storage.windows.get(window_id)
    if not window:
        logger.warning(f"Window not found: {window_id}")
        raise HTTPException(status_code=404, detail="Window not found")
    logger.debug(f"Retrieved {len(window.resources)} resources for window {window_id}")
    return window.resources

@router.get("/{window_id}/available-resources", response_model=List[Resource])
async def get_available_resources(
    window_id: str, 
    window_service: WindowService = Depends(get_window_service)
):
    logger.info(f"Fetching available resources for window: {window_id}")
    available = await window_service.get_available_resources(window_id)
    logger.debug(f"Retrieved {len(available)} available resources for window {window_id}")
    return available