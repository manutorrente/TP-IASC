import asyncio
from fastapi import FastAPI
import logging
from utils import async_request
from instances import AppInstance

logger = logging.getLogger(__name__)

async def poll_task(app: FastAPI):
    """Background task that polls instances"""
    config = app.state.config
    remote_peers = app.state.remote_peers
    interval = config.polling_interval
    
    while True:
        
        try:
            instances: list[AppInstance] = app.state.cluster.get_instances()
            await asyncio.gather(*[instance.is_healthy(remote_peers) for instance in instances])

            await asyncio.sleep(interval)
        except Exception as e:
            logger.critical(f"Critical error in polling task: {e}", exc_info=True)
            raise e

async def update_coordinator_to_peers(app: FastAPI):
    """Background task that polls remote sentinel peers"""
    config = app.state.config
    remote_peers = app.state.remote_peers
    interval = config.polling_interval
    
    while True:
        
        try:
            await remote_peers.coordinator_update()

            await asyncio.sleep(interval)
        except Exception as e:
            logger.critical(f"Critical error in peer polling task: {e}", exc_info=True)
            raise e

def start_background_task(app: FastAPI):
    """Start the background polling task"""
    loop = asyncio.get_event_loop()
    loop.create_task(poll_task(app))
    loop.create_task(update_coordinator_to_peers(app))