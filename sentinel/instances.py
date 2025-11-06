from collections import deque
import logging
from functools import lru_cache
from fastapi import Request
import httpx


from utils import async_request, AppMixin
from models import App
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from remote_peers import RemotePeers

logger = logging.getLogger(__name__)



class Cluster:
    def __init__(self, instances: list['AppInstance'] | None = None):
        self.instances = instances or []


    @lru_cache(maxsize=15)
    def get_instance(self, host: str, port: int) -> 'AppInstance | None':
        for instance in self.instances:
            if instance.host == host and instance.port == port:
                return instance
        return None
    
    def add_instance(self, host: str, port: int) -> None:
        instance = AppInstance(host, port)
        if not instance in self.instances:
            logger.info(f"Adding instance {host}:{port} to cluster")
            self.instances.append(instance)
        
    def add_instances(self, instances: list[App]) -> None:
        for inst in instances:
            self.add_instance(inst.host, inst.port)

    def get_instances_list(self) -> list[App]:
        return [App(host=instance.host, port=instance.port) for instance in self.instances]
    
    def get_instances(self) -> list['AppInstance']:
        return self.instances

def check_down_after(func):
    """Decorator to check objective down state after method execution."""
    async def wrapper(self, remote_peers: "RemotePeers"):
        logger.debug(f"Executing {func.__name__} for instance {self.host}:{self.port}")
        await func(self, remote_peers)
        await remote_peers.check_objectively_down(self)
    return wrapper

class AppInstance(AppMixin):
    def __init__(self, host: str, port: int):
        super().__init__(host, port)
        self.remote_down_counter = 0
        self.is_locally_up = True
        self.state_down_counter = 0
        

    async def get_health(self) -> str:
        url = f"{self.url}/health"
        try:
            response = await async_request("GET", url)
            status = response.get("status", "unknown")
        except httpx.HTTPError as e:
            status = "unreachable"
        return status

    async def is_healthy(self, remote_peers: "RemotePeers") -> None:
        status = await self.get_health()
        logger.debug(f"Checking health for instance {self.host}:{self.port}. Status: {status}")
        if status == "ok" and self.state_down_counter > 0:
            await self.mark_local_up(remote_peers)
            self.state_down_counter = 0
        elif status != "ok" and self.state_down_counter < 3:
            self.state_down_counter += 1
            logger.info(f"Instance {self.host}:{self.port} health check failed {self.state_down_counter} times.")
        elif status != "ok" and self.state_down_counter == 3:
            self.state_down_counter += 1
            await self.mark_local_down(remote_peers)
        elif status != "ok" and self.state_down_counter % 20:
            logger.info(f"Instance {self.host}:{self.port} health check failed {self.state_down_counter} times.")
        else:
            logger.debug(f"Instance {self.host}:{self.port} is healthy.")

    @check_down_after
    async def add_remote_down(self, remote_peers: "RemotePeers"):
        logger.info(f"Instance {self.host}:{self.port} reported as remotely down.")
        self.remote_down_counter += 1

    @check_down_after
    async def mark_local_down(self, remote_peers: "RemotePeers"):
        logger.warning(f"Instance {self.host}:{self.port} marked as locally down.")
        self.is_locally_up = False
        await remote_peers.add_local_down(self)

    @check_down_after
    async def mark_local_up(self, remote_peers: "RemotePeers"):
        logger.info(f"Instance {self.host}:{self.port} marked as locally up.")
        self.is_locally_up = True
        await remote_peers.remove_local_down(self)

    @check_down_after
    async def add_remote_up(self, remote_peers: "RemotePeers"):
        logger.info(f"Instance {self.host}:{self.port} reported as remotely up.")
        if self.remote_down_counter > 0:
            self.remote_down_counter -= 1
            
    def down_count(self) -> int:
        return self.is_locally_up + self.remote_down_counter
    
    def state(self) -> dict:
        return {
            "is_locally_up": self.is_locally_up,
            "remote_down_counter": self.remote_down_counter,
        }

