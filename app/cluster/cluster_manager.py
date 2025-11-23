import asyncio
import httpx
from typing import List, Optional
from datetime import datetime
from config import settings
import logging
from cluster.modification_history import modification_history
from enum import Enum
from dataclasses import dataclass
import jump
import uuid
import time

logger = logging.getLogger(__name__)

class ShardRole(Enum):
    MASTER = "master"
    REPLICA = "replica"

@dataclass
class Shard:
    shard_id: int
    role: ShardRole

    def __hash__(self):
        return hash(self.shard_id)

    def __eq__(self, other):
        if not isinstance(other, Shard):
            return False
        return self.shard_id == other.shard_id

class ClusterNode:
    def __init__(self, url: str):
        self.url = url
        self.blocked = False
        self.last_update: Optional[datetime] = None
        self.shards: List[Shard] = []
        
    def is_slave_of(self, shard_id: int) -> bool:
        for shard in self.shards:
            if shard.shard_id == shard_id and shard.role != ShardRole.MASTER:
                return True
        return False
    
    def is_master_of(self, shard_id: int) -> bool:
        for shard in self.shards:
            if shard.shard_id == shard_id and shard.role == ShardRole.MASTER:
                return True
        return False

class ClusterManager:
    def __init__(self):
        logger.debug("Initializing ClusterManager")
        self.shards: List[Shard] = []
        self.n_shards: int = 0
        self.cluster_nodes: List[ClusterNode] = []
        self._lock = asyncio.Lock()
        self._client = httpx.AsyncClient(timeout=15.0)
        self.get_self_url()
        logger.info(f"ClusterManager initialized with self_url: {self.self_url}")
        

    async def initialize(self):
        logger.info("Initializing cluster manager...")
        # Initial state will be empty, waiting for coordinator to push shard updates
        logger.info(f"Cluster manager initialized - waiting for shard assignments")

    def update_shard_state(self, shard_state: dict) -> None:
        """
        Update shard state from coordinator notification.
        
        Args:
            shard_state: Dictionary mapping instance URLs to their shard assignments
                        Format: {
                            "http://host:port": {
                                "master_shards": [0, 1],
                                "replica_shards": [2, 3]
                            },
                            ...
                        }
        """
        try:
            logger.info(f"Updating shard state with {len(shard_state)} instances")
            
            self_url = self.get_self_url()
            
            # Calculate total number of shards from the state
            all_shards = set()
            for instance_data in shard_state.values():
                all_shards.update(instance_data.get("master_shards", []))
                all_shards.update(instance_data.get("replica_shards", []))
            
            self.n_shards = len(all_shards) if all_shards else 0
            
            # Update this node's shard assignments
            if self_url in shard_state:
                self_data = shard_state[self_url]
                self.shards = []
                
                for shard_id in self_data.get("master_shards", []):
                    self.shards.append(Shard(shard_id=shard_id, role=ShardRole.MASTER))
                
                for shard_id in self_data.get("replica_shards", []):
                    self.shards.append(Shard(shard_id=shard_id, role=ShardRole.REPLICA))
                
                logger.info(f"Updated self shards: {[(s.shard_id, s.role.value) for s in self.shards]}")
            else:
                logger.warning(f"Self URL {self_url} not found in shard state")
                self.shards = []
            
            # Update cluster nodes and their shard assignments
            updated_nodes = []
            for instance_url, instance_data in shard_state.items():
                if instance_url == self_url:
                    continue
                
                # Find existing node or create new one
                existing_node = None
                for node in self.cluster_nodes:
                    if node.url == instance_url:
                        existing_node = node
                        break
                
                node = existing_node if existing_node else ClusterNode(instance_url)
                node.shards = []
                
                for shard_id in instance_data.get("master_shards", []):
                    node.shards.append(Shard(shard_id=shard_id, role=ShardRole.MASTER))
                
                for shard_id in instance_data.get("replica_shards", []):
                    node.shards.append(Shard(shard_id=shard_id, role=ShardRole.REPLICA))
                
                updated_nodes.append(node)
            
            self.cluster_nodes = updated_nodes
            
            logger.info(f"Shard state updated - Total shards: {self.n_shards}, Cluster nodes: {len(self.cluster_nodes)}")
            
        except Exception as e:
            logger.error(f"Error updating shard state: {e}", exc_info=True)
            raise

    def get_self_url(self) -> str:
        self.self_url = f"http://{settings.node_host}:{settings.node_port}"
        return self.self_url
    

    def _uuid_to_int(self, uuid_str: str) -> int:
        """Convert UUID string to integer for jump hash."""
        # Remove dashes and convert hex to int
        hex_str = uuid_str.replace('-', '')
        return int(hex_str, 16)

    def _get_shard_for_entity(self, entity_id: str) -> int:
        """Determine which shard an entity belongs to using jump hash."""
        if self.n_shards == 0:
            raise Exception("Cluster not ready: shard count is 0")
        
        # Convert entity_id to integer
        entity_int = self._uuid_to_int(entity_id)
        
        # Use jump hash algorithm
        shard_id = self._jump_hash(entity_int, self.n_shards)
        return shard_id
    
    def _jump_hash(self, key: int, num_buckets: int) -> int:
        """
        Jump consistent hash algorithm.
        Returns a bucket number in [0, num_buckets).
        """
        return jump.hash(key, num_buckets)

    def _is_master_of_shard(self, shard_id: int) -> bool:
        """Check if this node is the master for the given shard."""
        for shard in self.shards:
            if shard.shard_id == shard_id and shard.role == ShardRole.MASTER:
                return True
        return False

    def _is_responsible_for_entity(self, entity_id: str) -> bool:
        """Check if this node is responsible (master or replica) for the entity."""
        try:
            shard_id = self._get_shard_for_entity(entity_id)
            return any(s.shard_id == shard_id for s in self.shards)
        except Exception:
            return False
        
    def _is_master_for_entity(self, entity_id: str) -> bool:
        """Check if this node is the master for the entity."""
        try:
            shard_id = self._get_shard_for_entity(entity_id)
            return self._is_master_of_shard(shard_id)
        except Exception:
            return False

    def _get_available_nodes_for_shard(self, shard_id: int) -> List[ClusterNode]:
        """Returns list of unblocked replica nodes for a specific shard."""
        return [
            node for node in self.cluster_nodes 
            if not node.blocked and node.is_slave_of(shard_id)
        ]
    
    def _get_all_replica_nodes_for_shard(self, shard_id: int) -> List[ClusterNode]:
        """Returns list of ALL replica nodes for a specific shard (including blocked)."""
        return [
            node for node in self.cluster_nodes 
            if node.is_slave_of(shard_id)
        ]

    async def replicate_write(self, operation: str, entity_type: str, entity_id: str, data: dict) -> bool:
        """Replicate write operation to replica nodes of the same shard."""
        start_time = time.time()
        logger.info(f"[REPLICATION START] {entity_type}:{entity_id} - operation={operation}")
        
        # Determine which shard this entity belongs to
        try:
            shard_id = self._get_shard_for_entity(entity_id)
        except Exception as e:
            logger.error(f"Cannot determine shard for entity {entity_id}: {e}")
            raise
        
        # Check if this node is the master for this shard
        if not self._is_master_of_shard(shard_id):
            logger.debug(f"Not master of shard {shard_id}, cannot write")
            raise Exception(f"Node is not master of shard {shard_id}")
        
        logger.info(f"Replicating {operation} - Entity: {entity_type}:{entity_id} (Shard: {shard_id})")
        
        # Get available replica nodes for this shard
        available_nodes = self._get_available_nodes_for_shard(shard_id)
        required_acks = settings.write_quorum - 1
        
        logger.debug(f"Found {len(available_nodes)} available replica nodes for shard {shard_id}, need {required_acks} acks")
        logger.debug(f"Available nodes: {[node.url for node in available_nodes]}")
        
        # Check if we have enough available nodes to meet quorum
        if len(available_nodes) < required_acks:
            all_replicas = self._get_all_replica_nodes_for_shard(shard_id)
            logger.error(f"Insufficient available nodes for shard {shard_id}")
            logger.error(f"  Available replicas (unblocked): {len(available_nodes)}, Required: {required_acks}")
            logger.error(f"  Total replicas (including blocked): {len(all_replicas)}")
            logger.error(f"  Total cluster nodes: {len(self.cluster_nodes)}")
            logger.error(f"  Replica node details: {[(n.url, f'blocked={n.blocked}') for n in all_replicas]}")
            raise Exception(f"Insufficient available nodes to meet write quorum. Available: {len(available_nodes)}, Required: {required_acks}")
        
        logger.debug(f"Sending prepare to {len(available_nodes)} nodes for {entity_type}:{entity_id}")
        prepare_start = time.time()
        
        tasks = []
        target_nodes = available_nodes
        for node in target_nodes:
            tasks.append(self._send_prepare(node, operation, entity_type, entity_id, data))
        
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
               timeout=10.0
            )
            prepare_duration = time.time() - prepare_start
            logger.info(f"[PREPARE COMPLETE] {entity_type}:{entity_id} - duration={prepare_duration:.3f}s")
        except asyncio.TimeoutError:
            prepare_duration = time.time() - prepare_start
            logger.error(f"[PREPARE TIMEOUT] {entity_type}:{entity_id} - duration={prepare_duration:.3f}s (timeout=10.0s)")
            return False
        
        successful_nodes = [node for node, result in zip(target_nodes, results) 
                          if not isinstance(result, Exception) and result]
        
        if len(successful_nodes) < required_acks:
            logger.warning(f"Prepare phase failed - Successful: {len(successful_nodes)}, Required: {required_acks}")
            for node in successful_nodes:
                asyncio.create_task(self._send_abort(node, entity_type, entity_id))
            return False
        
        logger.info(f"Prepare phase successful, committing to {len(successful_nodes)} nodes")
        for node in successful_nodes:
            asyncio.create_task(self._send_commit(node, entity_type, entity_id))
        
        total_duration = time.time() - start_time
        logger.info(f"[REPLICATION END] {entity_type}:{entity_id} - total_duration={total_duration:.3f}s")
        return True

    async def _send_prepare(self, node: ClusterNode, operation: str, entity_type: str, entity_id: str, data: dict) -> bool:
        try:
            send_start = time.time()
            logger.debug(f"[PREPARE SEND START] {node.url} - {entity_type}:{entity_id}")
            
            response = await self._client.post(
                f"{node.url}/cluster/prepare",
                json={
                    "operation": operation,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "data": data
                }
            )
            send_duration = time.time() - send_start
            success = response.status_code == 200
            
            if not success:
                async with self._lock:
                    node.blocked = True
                logger.warning(f"[PREPARE FAILED] {node.url} - {entity_type}:{entity_id} - status={response.status_code}, duration={send_duration:.3f}s, node blocked")
            else:
                logger.debug(f"[PREPARE SUCCESS] {node.url} - {entity_type}:{entity_id} - status={response.status_code}, duration={send_duration:.3f}s")
            
            return success
        except asyncio.TimeoutError as e:
            send_duration = time.time() - send_start
            logger.warning(f"[PREPARE TIMEOUT] {node.url} - {entity_type}:{entity_id} - duration={send_duration:.3f}s - {type(e).__name__}")
            async with self._lock:
                node.blocked = True
            logger.warning(f"Node {node.url} blocked due to timeout")
            return False
        except Exception as e:
            send_duration = time.time() - send_start
            logger.warning(f"[PREPARE ERROR] {node.url} - {entity_type}:{entity_id} - duration={send_duration:.3f}s - {type(e).__name__}: {e}")
            async with self._lock:
                node.blocked = True
            logger.warning(f"Node {node.url} blocked due to exception")
            return False

    async def _send_commit(self, node: ClusterNode, entity_type: str, entity_id: str):
        try:
            logger.debug(f"Sending commit to {node.url} for {entity_type}:{entity_id}")
            response = await self._client.post(
                f"{node.url}/cluster/commit",
                json={
                    "entity_type": entity_type,
                    "entity_id": entity_id
                }
            )
            
            if response.status_code == 200:
                async with self._lock:
                    node.last_update = datetime.utcnow()
                logger.debug(f"Commit sent to {node.url}, updated timestamp")
            else:
                async with self._lock:
                    node.blocked = True
                logger.warning(f"Commit failed for {node.url}: {response.status_code}, node blocked")
        except Exception as e:
            logger.error(f"Error sending commit to {node.url}: {e}", exc_info=True)
            async with self._lock:
                node.blocked = True
            logger.warning(f"Node {node.url} blocked due to commit exception")

    async def _send_abort(self, node: ClusterNode, entity_type: str, entity_id: str):
        try:
            logger.debug(f"Sending abort to {node.url} for {entity_type}:{entity_id}")
            response = await self._client.post(
                f"{node.url}/cluster/abort",
                json={
                    "entity_type": entity_type,
                    "entity_id": entity_id
                }
            )
            logger.debug(f"Abort sent to {node.url}")
        except Exception as e:
            logger.error(f"Error sending abort to {node.url}: {e}", exc_info=True)

    async def shutdown(self):
        logger.info("Shutting down cluster manager...")
        if self._client:
            await self._client.aclose()
        logger.info("Cluster manager shutdown complete")

    async def send_batch_writes(self, node_url: str, since_timestamp: datetime | None) -> bool:
        """Send batch of modifications to a node to catch it up."""
        try:
            logger.info(f"Sending batch writes to {node_url} since {since_timestamp}")
            
            modifications = await modification_history.get_recent_modifications(since_timestamp)
            
            modifications = [mod for mod in modifications if self._is_responsible_for_entity(mod.entity_id)]
            
            if not modifications:
                logger.info(f"No modifications to send to {node_url}")
                return True
            
            batch_data = [
                {
                    "operation": mod.operation,
                    "entity_type": mod.entity_type,
                    "entity_id": mod.entity_id,
                    "data": mod.data.model_dump(mode='json') if hasattr(mod.data, 'model_dump') else mod.data,
                    "timestamp": mod.timestamp.isoformat()
                }
                for mod in modifications
            ]
            
            logger.debug(f"Sending {len(batch_data)} modifications to {node_url}")
            
            response = await self._client.post(
                f"{node_url}/cluster/batch-catchup",
                json={"modifications": batch_data}
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully sent {len(batch_data)} modifications to {node_url}")
                return True
            else:
                logger.warning(f"Failed to send batch to {node_url}: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending batch writes to {node_url}: {e}", exc_info=True)
            return False

    async def unblock_node(self, node_url: str) -> bool:
        """Unblock a node by URL. Returns True if node was found and unblocked."""
        async with self._lock:
            for node in self.cluster_nodes:
                if node.url == node_url:
                    node.blocked = False
                    logger.info(f"Node {node_url} unblocked")
                    return True
        logger.warning(f"Node {node_url} not found for unblocking")
        return False

    def get_status(self) -> dict:
        return {
            "n_shards": self.n_shards,
            "shards": [
                {
                    "shard_id": shard.shard_id,
                    "role": shard.role.value
                }
                for shard in self.shards
            ],
            "cluster_nodes": [
                {
                    "url": node.url,
                    "blocked": node.blocked,
                    "last_update": node.last_update.isoformat() if node.last_update else None,
                    "shards": [
                        {
                            "shard_id": shard.shard_id,
                            "role": shard.role.value
                        }
                        for shard in node.shards
                    ]
                }
                for node in self.cluster_nodes
            ],
            "total_nodes": len(self.cluster_nodes)
        }

cluster_manager = ClusterManager()