import asyncio
from fastapi import FastAPI
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def poll_task(app: FastAPI):
    """Background task that polls instances"""
    config = app.state.config
    cluster = app.state.cluster
    peers = app.state.peers
    
    interval = config.polling_interval
    
    while True:
        
        for instance in cluster.get_instances_list():
            pass
            
        await asyncio.sleep(interval)


def start_background_task(app: FastAPI):
    """Start the background polling task"""
    loop = asyncio.get_event_loop()
    loop.create_task(poll_task(app))