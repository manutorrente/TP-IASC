import httpx
import logging
from models import App, FailoverInfo
from utils import async_request, AppMixin, request_error_wrapper
from typing import TYPE_CHECKING
from instances import AppInstance
import asyncio


logger = logging.getLogger(__name__)

class RemotePeers:
    """
    Represents a collection of remote sentinel peers.
    """
    def __init__(self, self_peer: "RemoteSentinelPeer"):
        self.peers: list["RemoteSentinelPeer"] = []
        self.offline_peers: set["RemoteSentinelPeer"] = set()
        self.objectively_down_instances: set["AppInstance"] = set()
        self.locally_down_instances: set["AppInstance"] = set()
        self.self_peer = self_peer
        self.objective_coordinator = None
        self.votes_for_coordinator = set()

    def add_peer(self, peer: "RemoteSentinelPeer"):
        if peer not in self.peers:
            self.peers.append(peer)
        else:
            logger.warning(f"Peer {peer.host}:{peer.port} already exists in the list.")

    def get_peers(self) -> list["RemoteSentinelPeer"]:
        return self.peers
    
    def get_peer(self, host: str, port: int) -> "RemoteSentinelPeer | None":
        for peer in self.peers + [self.self_peer]:
            if peer.host == host and peer.port == port:
                return peer
        return None
    
    async def coordinator_update(self) -> None:
        """Notify all peers about the current objective coordinator"""
        results = await asyncio.gather(*[
            peer.coordinator_update(self.subjective_choose_coordinator_peer(), self.self_peer)
            for peer in self.peers
        ], return_exceptions=True)
        
        failed_peers = set()
        for peer, result in zip(self.peers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to notify coordinator update to peer {peer.url}: {result}. Peer is offline")
                failed_peers.add(peer)
  
        for peer in self.offline_peers - failed_peers:
            logger.info(f"Peer {peer.url} is back online.")
                
        self.offline_peers = failed_peers
            
    
    def change_coordinator(self, new_coordinator: App):
        peer = self.get_peer(new_coordinator.host, new_coordinator.port)
        if peer:
            logger.info(f"Changing objective coordinator to {new_coordinator.host}:{new_coordinator.port}")
            self.objective_coordinator = peer
        else:
            logger.warning(f"Coordinator peer {new_coordinator.host}:{new_coordinator.port} not found among remote peers.")
    
    async def incoming_coordinator_update(self, remote_coordinator_pick: App, origin: App):
        pick = self.get_peer(remote_coordinator_pick.host, remote_coordinator_pick.port)
        if pick == self.self_peer and origin not in self.votes_for_coordinator:
            logger.info(f"Received vote from {origin.host}:{origin.port} for self as coordinator")
            self.votes_for_coordinator.add(origin)
            
            if len(self.votes_for_coordinator) >= self.quorum_threshold():
                await self.declare_self_as_coordinator()
            
        if origin in self.votes_for_coordinator and pick != self.self_peer:
            logger.info(f"Removing vote from {origin.host}:{origin.port} for coordinator")    
            self.votes_for_coordinator.remove(origin)
        
    
    async def declare_self_as_coordinator(self):
        logger.info("Achieved quorum to become coordinator. Declaring self as coordinator.")
        self.objective_coordinator = self.self_peer
        await asyncio.gather(*(peer.notify_coordinator(self.self_peer) for peer in self.peers))
        
    
    
    def subjective_choose_coordinator_peer(self) -> "RemoteSentinelPeer":
        """Choose the coordinator peer based on lowest host and port"""
        peers = self.peers + [self.self_peer]
        sorted_peers = sorted(peers, key=lambda p: (p.host, p.port))
        sorted_peers = [peer for peer in sorted_peers if peer not in self.offline_peers]
        logger.info(f"Subjectively chosen coordinator is {sorted_peers[0].host}:{sorted_peers[0].port}")
        chosen = sorted_peers[0]
        if chosen == self.self_peer and self.self_peer not in self.votes_for_coordinator:
            logger.info("Self is the subjectively chosen coordinator.")
            self.votes_for_coordinator.add(self.self_peer)
        elif self.self_peer in self.votes_for_coordinator and chosen != self.self_peer:
            logger.info("Self is not the subjectively chosen coordinator anymore. Removing self vote.")
            self.votes_for_coordinator.remove(self.self_peer)
        return chosen
        
    
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

        if self.objective_coordinator == self.self_peer:
            logger.info(f"Self is the objective coordinator, initiating failover for instance {instance.host}:{instance.port}")
            self.failover
            
    async def failover(self, instance: "AppInstance"):
        failover_info = FailoverInfo(
            responsible_peer=App(host=self.self_peer.host, port=self.self_peer.port),
            failed_instance=App(host=instance.host, port=instance.port)
        )
        await self.notify_failover_start(failover_info)
        await asyncio.sleep(5)
        logger.info(f"Failover process initiated for instance {instance.host}:{instance.port}")
        await self.notify_failover_complete(failover_info)
        
    async def notify_failover_start(self, failover_info: FailoverInfo) -> None:
        for peer in self.peers + [self.self_peer]:
            try:
                await peer.notify_failover_start(failover_info)
            except httpx.HTTPError as e:
                logger.error(f"Failed to notify failover start to {peer.url}")
                logger.exception(e)    
    
    async def notify_failover_complete(self, failover_info: FailoverInfo) -> None:
        for peer in self.peers + [self.self_peer]:
            try:
                await peer.notify_failover_complete(failover_info)
            except httpx.HTTPError as e:
                logger.error(f"Failed to notify failover complete to {peer.url}")
                logger.exception(e)
    

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
        
    @request_error_wrapper
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
    
    async def coordinator_update(self, coordinator: "RemoteSentinelPeer", origin: "RemoteSentinelPeer") -> None:
        """Notify this peer about a coordinator update"""
        url = f"{self.url}/coordinator-update"
        payload = {
            "coordinator_pick": {"address": {"host": coordinator.host, "port": coordinator.port}},
            "origin": {"address": {"host": origin.host, "port": origin.port}}
        }
        await async_request("POST", url, json=payload)

    
    @request_error_wrapper
    async def notify_coordinator(self, coordinator: "RemoteSentinelPeer") -> None:
        """Notify this peer about the new coordinator"""
        url = f"{self.url}/coordinator-change"
        payload = {"address": {"host": coordinator.host, "port": coordinator.port}}
        await async_request("POST", url, json=payload)
        logger.info(f"Notified peer {self.url} that new coordinator is {coordinator.host}:{coordinator.port}")
        
    
    @request_error_wrapper
    async def notify_failover_complete(self, failover_info: FailoverInfo) -> None:
        """Notify this peer that failover has completed"""
        url = f"{self.url}/failover-complete"
        await async_request("POST", url, json=failover_info.model_dump())
        logger.info(f"Notified peer {self.url} that failover is complete")
        
    @request_error_wrapper        
    async def notify_failover_start(self, failover_info: FailoverInfo) -> None:
        """Notify this peer that failover is starting"""
        url = f"{self.url}/failover-start"
        await async_request("POST", url, json=failover_info.model_dump())
        logger.info(f"Notified peer {self.url} that failover is starting")