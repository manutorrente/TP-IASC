import asyncio
import httpx
from typing import List, Optional
from config import settings
import logging

logger = logging.getLogger(__name__)

class ClusterNode:
    def __init__(self, url: str):
        self.url = url

class ClusterManager:
    def __init__(self):
        logger.debug("Initializing ClusterManager")
        self.is_master: bool = False
        self.cluster_nodes: List[ClusterNode] = []
        self._lock = asyncio.Lock()
        self._client: Optional[httpx.AsyncClient] = None
        self.get_self_url()
        logger.info(f"ClusterManager initialized with self_url: {self.self_url}")
    
    async def initialize(self):
        logger.info("Initializing cluster manager...")
        self._client = httpx.AsyncClient(timeout=5.0)
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
    
    async def replicate_write(self, operation: str, entity_type: str, entity_id: str, data: dict) -> bool:
        if not self.is_master:
            logger.debug(f"Not master node, cant write")
            raise Exception("Node is not master")
        
        logger.info(f"Replicating {operation} - Entity: {entity_type}:{entity_id}")
        nodes = self.cluster_nodes
        required_acks = settings.write_quorum - 1
        
        
        logger.debug(f"Sending prepare to {required_acks} nodes")
        tasks = []
        for node in nodes[:required_acks]:
            tasks.append(self._send_prepare(node, operation, entity_type, entity_id, data))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful_nodes = [node for node, result in zip(nodes[:required_acks], results) 
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
    
    async def _send_prepare(self, node: ClusterNode, operation: str, entity_type: str, 
                           entity_id: str, data: dict) -> bool:
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
            logger.debug(f"Prepare response from {node.url}: {response.status_code}")
            return success
        except Exception as e:
            logger.warning(f"Error sending prepare to {node.url}: {e}")
            return False
    
    async def _send_commit(self, node: ClusterNode, entity_type: str, entity_id: str):
        try:
            logger.debug(f"Sending commit to {node.url} for {entity_type}:{entity_id}")
            await self._client.post(
                f"{node.url}/cluster/commit",
                json={
                    "entity_type": entity_type,
                    "entity_id": entity_id
                }
            )
            logger.debug(f"Commit sent to {node.url}")
        except Exception as e:
            logger.error(f"Error sending commit to {node.url}: {e}", exc_info=True)
    
    async def _send_abort(self, node: ClusterNode, entity_type: str, entity_id: str):
        try:
            logger.debug(f"Sending abort to {node.url} for {entity_type}:{entity_id}")
            await self._client.post(
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
        
    def get_status(self) -> dict:
        return {
            "is_master": self.is_master,
            "cluster_nodes": [node.url for node in self.cluster_nodes]
        }

cluster_manager = ClusterManager()