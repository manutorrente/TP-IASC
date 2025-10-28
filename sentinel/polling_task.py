import asyncio
from fastapi import FastAPI

async def poll_task(app: FastAPI):
    """Background task that polls instances"""
    config = app.state.config
    cluster = app.state.cluster
    peers = app.state.peers
    
    interval = config.polling_interval
    
    while True:
        print(f"Polling {len(cluster.get_instances_list())} instances...")
        
        for instance in cluster.get_instances_list():
            print(f"Checking {instance.host}:{instance.port}")
            
        await asyncio.sleep(interval)


def start_background_task(app: FastAPI):
    """Start the background polling task"""
    loop = asyncio.get_event_loop()
    loop.create_task(poll_task(app))