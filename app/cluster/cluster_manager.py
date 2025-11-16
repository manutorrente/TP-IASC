import asyncio
import httpx
from typing import List, Optional
from datetime import datetime
from config import settings
import logging
from cluster.modification_history import modification_history

logger = logging.getLogger(__name__)

class ClusterNode:
    def __init__(self, url: str):
        self.url = url
        self.blocked = False
        self.last_update: Optional[datetime] = None

class ClusterManager:
    def __init__(self):
        logger.debug("Initializing ClusterManager")
        self.is_master: bool = False
        self.cluster_nodes: List[ClusterNode] = []
        self._lock = asyncio.Lock()
        self._client = httpx.AsyncClient(timeout=5.0)
        self.get_self_url()
        logger.info(f"ClusterManager initialized with self_url: {self.self_url}")

    async def initialize(self):
        logger.info("Initializing cluster manager...")
        await self._fetch_cluster_info()
        logger.info(f"Cluster manager initialized - is_master: {self.is_master}, nodes: {len(self.cluster_nodes)}")

    async def _fetch_cluster_info(self):
        try:
            logger.debug(f"Fetching cluster info from {settings.coordinator_url}")
            response = await self._client.get(f"{settings.coordinator_url}/cluster-status")
            data = response.json()
            
            async with self._lock:
                master_node = data.get("master")
                self.is_master = (master_node == self.get_self_url())
                logger.debug(f"Cluster master node: {master_node}, self_url: {self.get_self_url()}, is_self_master: {self.is_master}")
                
                node_urls = data.get("health_status", {}).keys()
                self.cluster_nodes = [ClusterNode(url) for url in node_urls if url != self.get_self_url()]
                
            logger.info(f"Cluster info fetched - Master: {master_node}, Nodes: {len(self.cluster_nodes)}")
        except Exception as e:
            logger.error(f"Error fetching cluster info: {e}", exc_info=True)
            self.is_master = True
            self.cluster_nodes = []

    def get_self_url(self) -> str:
        self.self_url = f"http://{settings.node_host}:{settings.node_port}"
        return self.self_url

    def _get_available_nodes(self) -> List[ClusterNode]:
        """Returns list of unblocked nodes."""
        return [node for node in self.cluster_nodes if not node.blocked]

    async def replicate_write(self, operation: str, entity_type: str, entity_id: str, data: dict) -> bool:
        if not self.is_master:
            logger.debug(f"Not master node, cant write")
            raise Exception("Node is not master")
        
        logger.info(f"Replicating {operation} - Entity: {entity_type}:{entity_id}")
        
        # Get available (unblocked) nodes
        available_nodes = self._get_available_nodes()
        required_acks = settings.write_quorum - 1
        
        # Check if we have enough available nodes to meet quorum
        if len(available_nodes) < required_acks:
            logger.error(f"Insufficient available nodes - Available: {len(available_nodes)}, Required: {required_acks}")
            raise Exception(f"Insufficient available nodes to meet write quorum. Available: {len(available_nodes)}, Required: {required_acks}")
        
        logger.debug(f"Sending prepare to {required_acks} nodes from {len(available_nodes)} available")
        
        tasks = []
        target_nodes = available_nodes
        for node in target_nodes:
            tasks.append(self._send_prepare(node, operation, entity_type, entity_id, data))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
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
        
        return True

    async def _send_prepare(self, node: ClusterNode, operation: str, entity_type: str, entity_id: str, data: dict) -> bool:
        try:
            logger.debug(f"Sending prepare to {node.url} for {entity_type}:{entity_id}")
            response = await self._client.post(
                f"{node.url}/cluster/prepare",
                json={
                    "operation": operation,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "data": data
                }
            )
            success = response.status_code == 200
            
            if not success:
                async with self._lock:
                    node.blocked = True
                logger.warning(f"Prepare failed for {node.url}: {response.status_code}, node blocked")
            else:
                logger.debug(f"Prepare response from {node.url}: {response.status_code}")
            
            return success
        except Exception as e:
            logger.warning(f"Error sending prepare to {node.url}: {e}")
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
            
            if not modifications:
                logger.info(f"No modifications to send to {node_url}")
                return True
            
            batch_data = [
                {
                    "operation": mod.operation,
                    "entity_type": mod.entity_type,
                    "entity_id": mod.entity_id,
                    "data": mod.data.model_dump() if hasattr(mod.data, 'model_dump') else mod.data,
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
            "is_master": self.is_master,
            "cluster_nodes": [
                {
                    "url": node.url,
                    "blocked": node.blocked,
                    "last_update": node.last_update.isoformat() if node.last_update else None
                }
                for node in self.cluster_nodes
            ],
            "available_nodes": len(self._get_available_nodes()),
            "total_nodes": len(self.cluster_nodes)
        }

cluster_manager = ClusterManager()