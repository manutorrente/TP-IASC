import httpx
import logging
from utils import async_request, AppMixin, request_error_wrapper
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instances import AppInstance

logger = logging.getLogger(__name__)

class RemotePeers:
    """
    Represents a collection of remote sentinel peers.
    """
    def __init__(self):
        self.peers: list["RemoteSentinelPeer"] = []
        self.objectively_down_instances: set["AppInstance"] = set()
        self.locally_down_instances: set["AppInstance"] = set()

    def add_peer(self, peer: "RemoteSentinelPeer"):
        if peer not in self.peers:
            self.peers.append(peer)
        else:
            logger.warning(f"Peer {peer.host}:{peer.port} already exists in the list.")

    def get_peers(self) -> list["RemoteSentinelPeer"]:
        return self.peers
    
    async def notify_instance_down(self, instance: "AppInstance"):
        for peer in self.peers:
            await peer.notify_instance_down(instance)
            
    async def notify_instance_up(self, instance: "AppInstance"):
        for peer in self.peers:
            await peer.notify_instance_up(instance)
            
    def quorum_threshold(self) -> int:
        """Calculate the quorum threshold based on the number of peers"""
        return (len(self.peers) + 1) // 2 + 1
    
    async def objectively_down_action(self, instance: "AppInstance"):
        """Action to take when an instance is marked objectively down"""
        logger.warning(f"Instance {instance.host}:{instance.port} is objectively down.")
        pass
    
    async def add_local_down(self, instance: "AppInstance"):
        if instance not in self.locally_down_instances:
            self.locally_down_instances.add(instance)
            await self.notify_instance_down(instance)
    
    async def remove_local_down(self, instance: "AppInstance"):
        if instance in self.locally_down_instances:
            self.locally_down_instances.remove(instance)
            await self.notify_instance_up(instance)
    
    async def check_objectively_down(self, instance: "AppInstance"):
            
        
        if instance in self.objectively_down_instances and instance.down_count() < self.quorum_threshold():
            logger.info(f"Instance {instance.host}:{instance.port} recovered from objectively down state.")
            self.objectively_down_instances.remove(instance)
            return

        if instance not in self.objectively_down_instances and instance.down_count() >= self.quorum_threshold():
            logger.warning(f"Instance {instance.host}:{instance.port} marked as objectively down by quorum.")
            
    
        

class RemoteSentinelPeer(AppMixin):
    """
    Represents a peer sentinel node in the distributed system.
    Now uses dependency injection to get cluster and config data.
    """
        
    async def notify_instance_down(self, instance: "AppInstance"):
        """Notify this peer that an instance is down"""
        notify_url = f"{self.url}/instance-down"
        payload = {"host": instance.host, "port": instance.port}
        try:
            await async_request("POST", notify_url, json=payload)
            logger.info(f"Notified {self.url} about instance down: {instance.host}:{instance.port}")
        except httpx.HTTPError as e:
            logger.error(f"Failed to notify instance down to {self.url}")
            logger.exception(e)
            
    @request_error_wrapper
    async def notify_instance_up(self, instance: "AppInstance"):
        """Notify this peer that an instance is up"""
        notify_url = f"{self.url}/instance-up"
        payload = {"host": instance.host, "port": instance.port}
        await async_request("POST", notify_url, json=payload)
        logger.info(f"Notified {self.url} about instance up: {instance.host}:{instance.port}")


    @request_error_wrapper
    async def health_check(self) -> bool:
        """Check the health of this sentinel peer"""
        health_url = f"{self.url}/health"
        response = await async_request("GET", health_url)
        status = response.get("status", "unknown")
        return status == "ok"
