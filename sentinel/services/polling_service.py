import asyncio
from fastapi import FastAPI
import logging

from models.domain import AppInstance

logger = logging.getLogger(__name__)


async def poll_task(app: FastAPI):
    config = app.state.config
    peer_service = app.state.peer_service
    interval = config.polling_interval

    while True:
        try:
            instances: list[AppInstance] = app.state.cluster.get_instances()
            await asyncio.gather(*[instance.is_healthy(peer_service) for instance in instances])
            await asyncio.sleep(interval)
        except Exception as e:
            logger.critical(f"Critical error in polling task: {e}", exc_info=True)
            raise e


async def update_coordinator_to_peers(app: FastAPI):
    config = app.state.config
    peer_service = app.state.peer_service
    interval = config.polling_interval

    while True:
        try:
            await peer_service.coordinator_update()
            await asyncio.sleep(interval)
        except Exception as e:
            logger.critical(f"Critical error in peer polling task: {e}", exc_info=True)
            raise e


def start_background_tasks(app: FastAPI):
    loop = asyncio.get_event_loop()
    loop.create_task(poll_task(app))
    loop.create_task(update_coordinator_to_peers(app))
