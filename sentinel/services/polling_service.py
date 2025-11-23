import asyncio
from fastapi import FastAPI
import logging

from models.domain import AppInstance

logger = logging.getLogger(__name__)


async def poll_task(app: FastAPI):
    config = app.state.config
    peer_service = app.state.peer_service
    interval = config.polling_interval
    cluster = app.state.cluster

    while True:
        try:
            instances: list[AppInstance] = app.state.cluster.get_instances()
            await asyncio.gather(*[instance.is_healthy(peer_service) for instance in instances])
            await asyncio.sleep(interval*2)
            if peer_service.objective_coordinator == peer_service.self_peer:
                await cluster._notify_shard_changes()
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
            await asyncio.sleep(interval*3)
        except Exception as e:
            logger.critical(f"Critical error in peer polling task: {e}", exc_info=True)
            raise e


async def sync_cluster_state_to_peers(app: FastAPI):
    """Periodically sync cluster state with peer sentinels to ensure consistency"""
    config = app.state.config
    peer_service = app.state.peer_service
    cluster = app.state.cluster
    interval = config.polling_interval

    while True:
        try:
            await asyncio.sleep(interval * 3)
            if not peer_service.peers:
                logger.debug("No peer sentinels to sync with")
                continue
            
            local_instances = cluster.get_instances_list()
            logger.debug(f"Syncing cluster state with {len(peer_service.peers)} peers. Known instances: {len(local_instances)}")
            
            tasks = []
            for peer in peer_service.peers:
                tasks.append(peer_service._sync_instances_to_peer(peer, local_instances))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for peer, result in zip(peer_service.peers, results):
                if isinstance(result, Exception):
                    logger.warning(f"Failed to sync with peer {peer.host}:{peer.port}: {result}")
                elif isinstance(result, list):
                    before = len(cluster.instances)
                    await cluster.add_instances(result)
                    after = len(cluster.instances)
                    if after > before:
                        logger.info(f"Discovered {after - before} new instances from peer {peer.host}:{peer.port}")
                        
        except Exception as e:
            logger.error(f"Critical error in cluster sync task: {e}", exc_info=True)


def start_background_tasks(app: FastAPI):
    loop = asyncio.get_event_loop()
    loop.create_task(poll_task(app))
    loop.create_task(update_coordinator_to_peers(app))
    loop.create_task(sync_cluster_state_to_peers(app))
