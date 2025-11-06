import httpx
import logging
from models import App, FailoverInfo
from utils import async_request, AppMixin, request_error_wrapper
from typing import TYPE_CHECKING
from instances import AppInstance


logger = logging.getLogger(__name__)

class RemotePeers:
    """
    Represents a collection of remote sentinel peers.
    """
    def __init__(self, self_peer: "RemoteSentinelPeer"):
        self.peers: list["RemoteSentinelPeer"] = []
        self.objectively_down_instances: set["AppInstance"] = set()
        self.locally_down_instances: set["AppInstance"] = set()
        self.self_peer = self_peer

    def add_peer(self, peer: "RemoteSentinelPeer"):
        if peer not in self.peers:
            self.peers.append(peer)
        else:
            logger.warning(f"Peer {peer.host}:{peer.port} already exists in the list.")

    def get_peers(self) -> list["RemoteSentinelPeer"]:
        return self.peers
    
    def get_peer(self, host: str, port: int) -> "RemoteSentinelPeer | None":
        for peer in self.peers:
            if peer.host == host and peer.port == port:
                return peer
        return None
    
    def lowest_sorted_peer(self) -> "RemoteSentinelPeer":
        peers = self.peers + [self.self_peer]
        sorted_peers = sorted(peers, key=lambda p: (p.host, p.port))
        return sorted_peers[0]
    
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
        lowest_peer = self.lowest_sorted_peer()
        await lowest_peer.increase_failover_quorum(self, instance)
        for peer in self.peers:
            await peer.request_failover(lowest_peer)
    
    
    async def requested_failover(self, responsible_peer: App) -> None:
        peer = self.get_peer(responsible_peer.host, responsible_peer.port)
        if peer:
            logger.info(f"Requesting failover from responsible peer {responsible_peer.host}:{responsible_peer.port}")
            await peer.increase_failover_quorum(self, AppInstance(
                host=responsible_peer.host,
                port=responsible_peer.port
            ))
        else:
            logger.warning(f"Responsible peer {responsible_peer.host}:{responsible_peer.port} not found among remote peers.")
        
    async def notify_failover_complete(self, failover_info: FailoverInfo) -> None:
        for peer in self.peers + [self.self_peer]:
            try:
                await peer.notify_failover_complete(failover_info)
            except httpx.HTTPError as e:
                logger.error(f"Failed to notify failover complete to {peer.url}")
                logger.exception(e)
    
    async def update_after_failover(self, failover_info: FailoverInfo) -> None:
        app_instance = AppInstance(
            host=failover_info.failed_instance.host,
            port=failover_info.failed_instance.port
        )
        for peer in self.peers + [self.self_peer]:
            peer.reset_failover_quorum(app_instance)

    async def add_local_down(self, instance: "AppInstance"):
        if instance not in self.locally_down_instances:
            self.locally_down_instances.add(instance)
            await self.notify_instance_down(instance)
    
    async def remove_local_down(self, instance: "AppInstance"):
        if instance in self.locally_down_instances:
            self.locally_down_instances.remove(instance)
            await self.notify_instance_up(instance)
    
    async def check_objectively_down(self, instance: "AppInstance"):
            
        logger.info(f"Checking objectively down status for instance {instance.host}:{instance.port}")
        
        if instance in self.objectively_down_instances and instance.down_count() < self.quorum_threshold():
            logger.info(f"Instance {instance.host}:{instance.port} recovered from objectively down state.")
            self.objectively_down_instances.remove(instance)
            return

        if (instance not in self.objectively_down_instances) and (instance.down_count() >= self.quorum_threshold()):
            logger.warning(f"Instance {instance.host}:{instance.port} marked as objectively down by quorum.")
            self.objectively_down_instances.add(instance)
            await self.objectively_down_action(instance)
            
    
        

class RemoteSentinelPeer(AppMixin):
    """
    Represents a peer sentinel node in the distributed system.
    Now uses dependency injection to get cluster and config data.
    """
    
    def __init__(self, host: str, port: int):
        super().__init__(host, port)
        self.failover_quorum = {}
        
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

    async def increase_failover_quorum(self, remote_peers: "RemotePeers", instance: "AppInstance") -> None:
        self.failover_quorum[instance.url] = self.failover_quorum.get(instance.url, 0) + 1
        logger.info(f"Failover quorum for peer {self.url} increased to {self.failover_quorum}")
        if self.failover_quorum[instance.url] >= remote_peers.quorum_threshold():
            await self.failover(instance)
            
    def reset_failover_quorum(self, instance: "AppInstance") -> None:
        if instance.url in self.failover_quorum:
            del self.failover_quorum[instance.url]
            logger.info(f"Failover quorum for peer {self.url} reset for instance {instance.url}")

    @request_error_wrapper
    async def failover(self, instance: "AppInstance") -> None:
        """Initiate failover for this sentinel peer"""
        failover_url = f"{self.url}/failover"
        await async_request("POST", failover_url, json={"host": instance.host, "port": instance.port})
        logger.info(f"Initiated failover on peer {self.url}")

    @request_error_wrapper
    async def request_failover(self, responsible_peer: "RemoteSentinelPeer") -> None:
        """Request failover from the responsible peer"""
        url = f"{responsible_peer.url}/request-failover"
        payload = {"host": responsible_peer.host, "port": responsible_peer.port}
        await async_request("POST", url, json=payload)
        
    
    @request_error_wrapper
    async def notify_failover_complete(self, failover_info: FailoverInfo) -> None:
        """Notify this peer that failover has completed"""
        url = f"{self.url}/failover-complete"
        await async_request("POST", url, json=failover_info.model_dump())
        logger.info(f"Notified peer {self.url} that failover is complete")