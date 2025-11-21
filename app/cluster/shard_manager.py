"""
Shard Manager Module

Manages shard assignments, replica placement, and routing.
Each shard has N/2+1 replicas with one master.
"""

import asyncio
import httpx
import hashlib
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from config import settings
import logging

logger = logging.getLogger(__name__)


@dataclass
class ShardNode:
    """Represents a node in the cluster"""
    node_id: str
    url: str
    is_alive: bool = True
    last_heartbeat: Optional[datetime] = None
    shards: Set[int] = field(default_factory=set)  # Shard IDs this node has replicas for
    master_shards: Set[int] = field(default_factory=set)  # Shard IDs where this node is master
    
    def __hash__(self):
        return hash(self.node_id)
    
    def __eq__(self, other):
        return self.node_id == other.node_id


@dataclass
class ShardInfo:
    """Information about a shard"""
    shard_id: int
    master_node: Optional[ShardNode] = None
    replica_nodes: List[ShardNode] = field(default_factory=list)
    
    def get_all_nodes(self) -> List[ShardNode]:
        """Get all nodes (master + replicas) for this shard"""
        nodes = []
        if self.master_node:
            nodes.append(self.master_node)
        nodes.extend([n for n in self.replica_nodes if n != self.master_node])
        return nodes
    
    def get_healthy_replicas(self) -> List[ShardNode]:
        """Get all healthy replica nodes (excluding master)"""
        return [n for n in self.replica_nodes if n.is_alive and n != self.master_node]
    
    def has_quorum(self, num_nodes: int) -> bool:
        """Check if shard has quorum of healthy nodes"""
        required = (num_nodes // 2) + 1
        healthy = sum(1 for n in self.get_all_nodes() if n.is_alive)
        return healthy >= required


class ShardManager:
    """
    Manages sharding across the cluster.
    
    Responsibilities:
    - Assign shards to nodes with N/2+1 replication
    - Track master for each shard
    - Route operations to correct shard master
    - Handle shard master failover
    - Rebalance shards when nodes join/leave
    """
    
    def __init__(self, num_shards: int, self_url: str):
        """
        Initialize shard manager.
        
        Args:
            num_shards: Total number of shards
            self_url: URL of this node
        """
        self.num_shards = num_shards
        self.self_url = self_url
        self.nodes: Dict[str, ShardNode] = {}  # node_url -> ShardNode
        self.shards: Dict[int, ShardInfo] = {}  # shard_id -> ShardInfo
        self._lock = asyncio.Lock()
        self._client = httpx.AsyncClient(timeout=5.0)
        
        # Initialize shards
        for shard_id in range(num_shards):
            self.shards[shard_id] = ShardInfo(shard_id=shard_id)
        
        logger.info(f"ShardManager initialized with {num_shards} shards, self_url: {self_url}")
    
    async def initialize(self, cluster_nodes: List[str]):
        """
        Initialize shard assignments across cluster nodes.
        
        Args:
            cluster_nodes: List of node URLs in the cluster
        """
        async with self._lock:
            # Register all nodes
            for node_url in cluster_nodes:
                node_id = self._generate_node_id(node_url)
                self.nodes[node_url] = ShardNode(
                    node_id=node_id,
                    url=node_url,
                    is_alive=True,
                    last_heartbeat=datetime.utcnow()
                )
            
            # Assign shards to nodes
            await self._assign_shards()
            
            logger.info(f"ShardManager initialized with {len(self.nodes)} nodes")
            self._log_shard_distribution()
    
    def _generate_node_id(self, node_url: str) -> str:
        """Generate deterministic node ID from URL"""
        return hashlib.sha256(node_url.encode()).hexdigest()[:16]
    
    async def _assign_shards(self):
        """
        Assign shards to nodes using consistent hashing.
        Each shard gets N/2+1 replicas.
        """
        num_nodes = len(self.nodes)
        if num_nodes == 0:
            logger.warning("No nodes available for shard assignment")
            return
        
        min_replicas = (num_nodes // 2) + 1
        logger.info(f"Assigning shards with {min_replicas} replicas per shard")
        
        for shard_id in range(self.num_shards):
            # Calculate node distances for this shard
            node_distances = []
            for node in self.nodes.values():
                combined = f"shard_{shard_id}_node_{node.node_id}"
                distance = int(hashlib.sha256(combined.encode()).hexdigest(), 16)
                node_distances.append((distance, node))
            
            # Sort by distance and assign to closest nodes
            node_distances.sort(key=lambda x: x[0])
            
            replica_nodes = []
            for i in range(min(min_replicas, len(node_distances))):
                node = node_distances[i][1]
                node.shards.add(shard_id)
                replica_nodes.append(node)
                
                # First node is master
                if i == 0:
                    node.master_shards.add(shard_id)
                    self.shards[shard_id].master_node = node
                    logger.info(f"Shard {shard_id}: Master assigned to {node.url}")
            
            self.shards[shard_id].replica_nodes = replica_nodes
            logger.debug(f"Shard {shard_id}: {len(replica_nodes)} replicas assigned")
    
    def get_shard_master(self, shard_id: int) -> Optional[ShardNode]:
        """Get the master node for a shard"""
        if shard_id not in self.shards:
            return None
        return self.shards[shard_id].master_node
    
    def get_shard_replicas(self, shard_id: int) -> List[ShardNode]:
        """Get all replica nodes for a shard"""
        if shard_id not in self.shards:
            return []
        return self.shards[shard_id].replica_nodes
    
    def is_master_for_shard(self, shard_id: int) -> bool:
        """Check if this node is master for the given shard"""
        master = self.get_shard_master(shard_id)
        return master is not None and master.url == self.self_url
    
    def is_replica_for_shard(self, shard_id: int) -> bool:
        """Check if this node is a replica for the given shard"""
        replicas = self.get_shard_replicas(shard_id)
        return any(node.url == self.self_url for node in replicas)
    
    def get_my_shards(self) -> Set[int]:
        """Get shard IDs that this node is responsible for"""
        for node in self.nodes.values():
            if node.url == self.self_url:
                return node.shards
        return set()
    
    def get_my_master_shards(self) -> Set[int]:
        """Get shard IDs where this node is master"""
        for node in self.nodes.values():
            if node.url == self.self_url:
                return node.master_shards
        return set()
    
    async def replicate_to_shard(
        self, 
        shard_id: int, 
        operation: str, 
        entity_type: str, 
        entity_id: str, 
        data: dict
    ) -> bool:
        """
        Replicate write operation to shard replicas.
        
        Args:
            shard_id: Target shard ID
            operation: Operation type (add, update, delete)
            entity_type: Type of entity
            entity_id: Entity identifier
            data: Entity data
            
        Returns:
            True if quorum achieved, False otherwise
        """
        if not self.is_master_for_shard(shard_id):
            raise Exception(f"Node is not master for shard {shard_id}")
        
        shard_info = self.shards[shard_id]
        replicas = shard_info.get_healthy_replicas()
        
        num_nodes = len(self.nodes)
        required_acks = (num_nodes // 2) + 1 - 1  # -1 because master counts
        
        if len(replicas) < required_acks:
            logger.error(
                f"Insufficient replicas for shard {shard_id}. "
                f"Available: {len(replicas)}, Required: {required_acks}"
            )
            raise Exception(f"Insufficient replicas to meet write quorum for shard {shard_id}")
        
        logger.info(f"Replicating to shard {shard_id}: {operation} {entity_type}:{entity_id}")
        
        # Send prepare to replicas
        tasks = []
        for replica in replicas:
            tasks.append(self._send_shard_prepare(replica, shard_id, operation, entity_type, entity_id, data))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful_replicas = [
            replica for replica, result in zip(replicas, results)
            if not isinstance(result, Exception) and result
        ]
        
        if len(successful_replicas) < required_acks:
            logger.warning(
                f"Shard {shard_id} prepare failed. "
                f"Successful: {len(successful_replicas)}, Required: {required_acks}"
            )
            # Abort on failed replicas
            for replica in successful_replicas:
                asyncio.create_task(self._send_shard_abort(replica, shard_id, entity_type, entity_id))
            return False
        
        # Commit to successful replicas
        logger.info(f"Shard {shard_id} prepare successful, committing to {len(successful_replicas)} replicas")
        for replica in successful_replicas:
            asyncio.create_task(self._send_shard_commit(replica, shard_id, entity_type, entity_id))
        
        return True
    
    async def _send_shard_prepare(
        self, 
        node: ShardNode, 
        shard_id: int,
        operation: str, 
        entity_type: str, 
        entity_id: str, 
        data: dict
    ) -> bool:
        """Send prepare message to a replica node"""
        try:
            response = await self._client.post(
                f"{node.url}/cluster/shard-prepare",
                json={
                    "shard_id": shard_id,
                    "operation": operation,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "data": data
                }
            )
            success = response.status_code == 200
            if not success:
                logger.warning(f"Shard {shard_id} prepare failed for {node.url}: {response.status_code}")
            return success
        except Exception as e:
            logger.warning(f"Error sending shard prepare to {node.url}: {e}")
            return False
    
    async def _send_shard_commit(
        self, 
        node: ShardNode, 
        shard_id: int,
        entity_type: str, 
        entity_id: str
    ):
        """Send commit message to a replica node"""
        try:
            await self._client.post(
                f"{node.url}/cluster/shard-commit",
                json={
                    "shard_id": shard_id,
                    "entity_type": entity_type,
                    "entity_id": entity_id
                }
            )
            logger.debug(f"Shard {shard_id} commit sent to {node.url}")
        except Exception as e:
            logger.error(f"Error sending shard commit to {node.url}: {e}")
    
    async def _send_shard_abort(
        self, 
        node: ShardNode, 
        shard_id: int,
        entity_type: str, 
        entity_id: str
    ):
        """Send abort message to a replica node"""
        try:
            await self._client.post(
                f"{node.url}/cluster/shard-abort",
                json={
                    "shard_id": shard_id,
                    "entity_type": entity_type,
                    "entity_id": entity_id
                }
            )
            logger.debug(f"Shard {shard_id} abort sent to {node.url}")
        except Exception as e:
            logger.error(f"Error sending shard abort to {node.url}: {e}")
    
    async def handle_shard_master_failure(self, shard_id: int, failed_node_url: str):
        """
        Handle failure of a shard master.
        Elect new master from healthy replicas.
        
        Args:
            shard_id: Shard that lost its master
            failed_node_url: URL of failed master node
        """
        async with self._lock:
            shard_info = self.shards[shard_id]
            
            if shard_info.master_node and shard_info.master_node.url == failed_node_url:
                logger.warning(f"Shard {shard_id} master failed: {failed_node_url}")
                
                # Mark node as down
                if failed_node_url in self.nodes:
                    self.nodes[failed_node_url].is_alive = False
                    self.nodes[failed_node_url].master_shards.discard(shard_id)
                
                # Elect new master from healthy replicas
                healthy_replicas = [n for n in shard_info.replica_nodes if n.is_alive and n.url != failed_node_url]
                
                if healthy_replicas:
                    # Choose replica with lowest node_id (deterministic)
                    healthy_replicas.sort(key=lambda n: n.node_id)
                    new_master = healthy_replicas[0]
                    
                    new_master.master_shards.add(shard_id)
                    shard_info.master_node = new_master
                    
                    logger.info(f"Shard {shard_id} new master elected: {new_master.url}")
                    
                    # Notify other nodes about new master
                    await self._broadcast_new_shard_master(shard_id, new_master)
                else:
                    logger.error(f"Shard {shard_id} has no healthy replicas for failover")
                    shard_info.master_node = None
    
    async def _broadcast_new_shard_master(self, shard_id: int, new_master: ShardNode):
        """Broadcast new shard master to all nodes"""
        tasks = []
        for node in self.nodes.values():
            if node.is_alive and node.url != self.self_url:
                tasks.append(self._notify_new_shard_master(node, shard_id, new_master))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _notify_new_shard_master(self, node: ShardNode, shard_id: int, new_master: ShardNode):
        """Notify a node about new shard master"""
        try:
            await self._client.post(
                f"{node.url}/cluster/new-shard-master",
                json={
                    "shard_id": shard_id,
                    "master_url": new_master.url
                }
            )
            logger.debug(f"Notified {node.url} about new master for shard {shard_id}")
        except Exception as e:
            logger.error(f"Error notifying {node.url} about new shard master: {e}")
    
    def _log_shard_distribution(self):
        """Log current shard distribution for debugging"""
        logger.info("=== Shard Distribution ===")
        for node in self.nodes.values():
            logger.info(
                f"Node {node.url}: "
                f"Shards={sorted(node.shards)}, "
                f"Master for={sorted(node.master_shards)}"
            )
        
        for shard_id, shard_info in self.shards.items():
            master_url = shard_info.master_node.url if shard_info.master_node else "None"
            replica_urls = [n.url for n in shard_info.replica_nodes]
            logger.info(f"Shard {shard_id}: Master={master_url}, Replicas={replica_urls}")
    
    def get_status(self) -> dict:
        """Get current shard manager status"""
        return {
            "num_shards": self.num_shards,
            "num_nodes": len(self.nodes),
            "my_shards": sorted(self.get_my_shards()),
            "my_master_shards": sorted(self.get_my_master_shards()),
            "shards": [
                {
                    "shard_id": shard_id,
                    "master": info.master_node.url if info.master_node else None,
                    "replicas": [n.url for n in info.replica_nodes],
                    "has_quorum": info.has_quorum(len(self.nodes))
                }
                for shard_id, info in self.shards.items()
            ]
        }
    
    async def shutdown(self):
        """Shutdown shard manager"""
        logger.info("Shutting down shard manager...")
        if self._client:
            await self._client.aclose()
        logger.info("Shard manager shutdown complete")


# Global shard manager instance
shard_manager: Optional[ShardManager] = None


def initialize_shard_manager(num_shards: int, self_url: str) -> ShardManager:
    """Initialize global shard manager"""
    global shard_manager
    shard_manager = ShardManager(num_shards, self_url)
    return shard_manager


def get_shard_manager() -> ShardManager:
    """Get global shard manager instance"""
    if shard_manager is None:
        raise RuntimeError("ShardManager not initialized")
    return shard_manager
