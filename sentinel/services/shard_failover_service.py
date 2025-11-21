"""
Shard-Aware Failover Service for Sentinel

Extends sentinel failover to handle per-shard master failures.
Coordinates shard master election across sentinels.
"""

import logging
import asyncio
import httpx
from typing import Dict, Set, Optional, List
from models.api import App

logger = logging.getLogger(__name__)


class ShardFailoverService:
    """
    Manages shard-specific failover operations.
    
    When a node fails, this service:
    1. Identifies which shards lost their master
    2. Coordinates election of new shard masters
    3. Notifies remaining nodes about new shard masters
    """
    
    def __init__(self, num_shards: int = 4):
        self.num_shards = num_shards
        self.shard_masters: Dict[int, Optional[App]] = {i: None for i in range(num_shards)}
        self.shard_replicas: Dict[int, List[App]] = {i: [] for i in range(num_shards)}
        self._client = httpx.AsyncClient(timeout=5.0)
        logger.info(f"ShardFailoverService initialized with {num_shards} shards")
    
    async def register_shard_topology(self, shard_id: int, master: App, replicas: List[App]):
        """
        Register the topology for a shard.
        
        Args:
            shard_id: Shard identifier
            master: Current master node for the shard
            replicas: List of replica nodes for the shard
        """
        self.shard_masters[shard_id] = master
        self.shard_replicas[shard_id] = replicas
        logger.info(
            f"Registered topology for shard {shard_id}: "
            f"Master={master.host}:{master.port}, "
            f"Replicas={len(replicas)}"
        )
    
    async def handle_node_failure(self, failed_instance: App) -> List[int]:
        """
        Handle failure of a node and identify affected shards.
        
        Args:
            failed_instance: The failed node
            
        Returns:
            List of shard IDs that lost their master
        """
        affected_shards = []
        
        for shard_id, master in self.shard_masters.items():
            if master and master.host == failed_instance.host and master.port == failed_instance.port:
                logger.warning(f"Shard {shard_id} master failed: {failed_instance.host}:{failed_instance.port}")
                affected_shards.append(shard_id)
        
        return affected_shards
    
    async def elect_new_shard_master(self, shard_id: int, failed_master: App) -> Optional[App]:
        """
        Elect a new master for a shard after master failure.
        
        Args:
            shard_id: Shard that lost its master
            failed_master: The failed master node
            
        Returns:
            New master node, or None if no healthy replicas available
        """
        replicas = self.shard_replicas.get(shard_id, [])
        
        # Filter out the failed master
        healthy_replicas = [
            r for r in replicas 
            if not (r.host == failed_master.host and r.port == failed_master.port)
        ]
        
        if not healthy_replicas:
            logger.error(f"Shard {shard_id} has no healthy replicas for failover")
            return None
        
        # Check health of replicas
        verified_replicas = []
        for replica in healthy_replicas:
            if await self._check_node_health(replica):
                verified_replicas.append(replica)
        
        if not verified_replicas:
            logger.error(f"Shard {shard_id} has no verified healthy replicas")
            return None
        
        # Choose replica with lowest (host, port) tuple (deterministic)
        verified_replicas.sort(key=lambda r: (r.host, r.port))
        new_master = verified_replicas[0]
        
        logger.info(f"Elected new master for shard {shard_id}: {new_master.host}:{new_master.port}")
        
        # Update local state
        self.shard_masters[shard_id] = new_master
        
        return new_master
    
    async def _check_node_health(self, node: App) -> bool:
        """Check if a node is healthy"""
        try:
            url = f"http://{node.host}:{node.port}/health"
            response = await self._client.get(url)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Health check failed for {node.host}:{node.port}: {e}")
            return False
    
    async def notify_new_shard_master(self, shard_id: int, new_master: App, nodes: List[App]):
        """
        Notify all nodes about the new shard master.
        
        Args:
            shard_id: Shard ID
            new_master: New master node
            nodes: List of all nodes to notify
        """
        logger.info(f"Notifying nodes about new master for shard {shard_id}")
        
        tasks = []
        for node in nodes:
            # Don't notify the new master itself
            if node.host == new_master.host and node.port == new_master.port:
                continue
            tasks.append(self._send_new_master_notification(node, shard_id, new_master))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful = sum(1 for r in results if not isinstance(r, Exception))
            logger.info(f"Notified {successful}/{len(tasks)} nodes about new shard {shard_id} master")
    
    async def _send_new_master_notification(self, node: App, shard_id: int, new_master: App):
        """Send new master notification to a node"""
        try:
            url = f"http://{node.host}:{node.port}/cluster/new-shard-master"
            await self._client.post(
                url,
                json={
                    "shard_id": shard_id,
                    "master_url": f"http://{new_master.host}:{new_master.port}"
                }
            )
            logger.debug(f"Notified {node.host}:{node.port} about shard {shard_id} new master")
        except Exception as e:
            logger.error(f"Failed to notify {node.host}:{node.port}: {e}")
            raise
    
    async def perform_shard_failover(self, failed_instance: App, all_nodes: List[App]):
        """
        Perform complete shard failover for a failed node.
        
        Args:
            failed_instance: The failed node
            all_nodes: List of all nodes in the cluster
        """
        logger.info(f"Starting shard failover for failed node: {failed_instance.host}:{failed_instance.port}")
        
        # Identify affected shards
        affected_shards = await self.handle_node_failure(failed_instance)
        
        if not affected_shards:
            logger.info("No shards affected by node failure")
            return
        
        logger.info(f"Shards affected by failure: {affected_shards}")
        
        # Elect new masters for affected shards
        for shard_id in affected_shards:
            new_master = await self.elect_new_shard_master(shard_id, failed_instance)
            
            if new_master:
                # Notify all nodes about new master
                await self.notify_new_shard_master(shard_id, new_master, all_nodes)
                logger.info(f"Shard {shard_id} failover complete. New master: {new_master.host}:{new_master.port}")
            else:
                logger.error(f"Shard {shard_id} failover failed - no healthy replicas")
    
    def get_shard_status(self) -> Dict:
        """Get current status of all shards"""
        return {
            "num_shards": self.num_shards,
            "shards": [
                {
                    "shard_id": shard_id,
                    "master": f"{master.host}:{master.port}" if master else None,
                    "num_replicas": len(self.shard_replicas.get(shard_id, []))
                }
                for shard_id, master in self.shard_masters.items()
            ]
        }
    
    async def shutdown(self):
        """Shutdown the service"""
        logger.info("Shutting down ShardFailoverService...")
        if self._client:
            await self._client.aclose()
        logger.info("ShardFailoverService shutdown complete")
