import httpx
from collections import deque
import logging
from functools import lru_cache
from fastapi import Request

from .models import App

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def async_request(method: str, url: str, **kwargs):
    async with httpx.AsyncClient() as client:
        response = await client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

class Cluster:
    def __init__(self, instances: list['AppInstance'] | None = None):
        self.instances = instances or []

    async def check_health(self):
        results = {}
        for instance in self.instances:
            is_healthy = await instance.is_healthy()
            results[f"{instance.host}:{instance.port}"] = is_healthy
        return results
    
    @lru_cache(maxsize=15)
    def get_instance(self, host: str, port: int) -> 'AppInstance | None':
        for instance in self.instances:
            if instance.host == host and instance.port == port:
                return instance
        return None
    
    def add_instance(self, host: str, port: int) -> None:
        logger.info(f"Adding instance {host}:{port} to cluster")
        instance = AppInstance(host, port)
        self.instances.append(instance)
        
    def add_instances(self, instances: list[App]) -> None:
        for inst in instances:
            host = inst.host
            port = inst.port
            if host and port and not self.get_instance(host, port):
                self.add_instance(host, port)

    def get_instances_list(self) -> list[App]:
        return [App(host=instance.host, port=instance.port) for instance in self.instances]
    
    def get_instances(self) -> list['AppInstance']:
        return self.instances


class AppInstance:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.states = deque(maxlen=30)
        self.down_counter = 0

    async def get_health(self):
        url = f"http://{self.host}:{self.port}/health"
        try:
            response = await async_request("GET", url)
            status = response.get("status", "unknown")
        except httpx.HTTPError as e:
            status = "unreachable"
        self.states.append(status)
        
    async def is_healthy(self) -> bool:
        await self.get_health()
        recent_states = list(self.states)[-3:]
        state = all(state == "ok" for state in recent_states)
        if not state:
            logger.warning(f"Instance {self.host}:{self.port} is down. Recent states: {recent_states}")
        return state

class SentinelCluster:
    
    def __init__(self) -> None:
        self.peers: list[SentinelPeer] = []
        
    def add_peer(self, host: str, port: int) -> None:
        peer = SentinelPeer(host, port)
        if not any(p.host == host and p.port == port for p in self.peers):
            logger.info(f"Adding sentinel peer {peer.host}:{peer.port}")
            self.peers.append(peer)
        
    async def discover_all_peers(self, local_host: str, local_port: int, cluster: Cluster) -> list[App]:
        discovered_apps = []
        for peer in self.peers:
            new_apps = await peer.discover_peer(local_host, local_port, cluster)
            discovered_apps.extend(new_apps)
        return discovered_apps

class SentinelPeer:
    """
    Represents a peer sentinel node in the distributed system.
    Now uses dependency injection to get cluster and config data.
    """
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.url = f"http://{self.host}:{self.port}"
        
    async def notify_instance_down(self, instance: AppInstance):
        """Notify this peer that an instance is down"""
        notify_url = f"{self.url}/instance-down"
        payload = {"host": instance.host, "port": instance.port}
        try:
            await async_request("POST", notify_url, json=payload)
            logger.info(f"Notified {self.url} about instance down: {instance.host}:{instance.port}")
        except httpx.HTTPError as e:
            logger.error(f"Failed to notify instance down to {self.url}")
            logger.exception(e)
            
    async def discover_peer(self, local_host: str, local_port: int, cluster: Cluster) -> list[App]:
        """
        Discover this peer and exchange cluster information.
        
        Args:
            local_host: This sentinel's host
            local_port: This sentinel's port
            cluster: The local cluster to share with the peer
        
        Returns:
            List of App instances from the peer's cluster
        """
        discover_url = f"{self.url}/discover-peer"
        payload = {
            "host": local_host,
            "port": local_port,
            "instances": [app.model_dump() for app in cluster.get_instances_list()]
        }
        try:
            response = await async_request("POST", discover_url, json=payload)
            instances_data = response.get("local_instances", [])
            logger.info(f"Discovered {len(instances_data)} instances from peer {self.url}")
            return [App(**instance) for instance in instances_data]
        except httpx.HTTPError as e:
            logger.error(f"Failed to discover peer at {self.url}")
            logger.exception(e)
            return []