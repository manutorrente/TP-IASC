import asyncio
import httpx
from datetime import datetime, timedelta
from cluster.cluster_manager import cluster_manager, ClusterNode
from storage import Storage
from config import settings
import logging

logger = logging.getLogger(__name__)

class NodeRecoveryService:
    def __init__(self):
        self._running = False
        self._task = None
        self._client: httpx.AsyncClient = None
        self._storage: Storage = None
        self.check_interval = getattr(settings, 'health_check_interval_seconds', 30)
    
    def set_storage(self, storage: Storage):
        """Set the storage instance."""
        self._storage = storage
        logger.info("Storage set for health check service")
    
    async def start(self):
        """Start the health check service background task."""
        if self._running:
            logger.warning("Health check service already running")
            return
        
        if not self._storage:
            logger.error("Cannot start health check service: storage not set")
            raise RuntimeError("Storage must be set before starting health check service")
        
        logger.info("Starting health check service")
        self._running = True
        self._client = httpx.AsyncClient(timeout=5.0)
        self._task = asyncio.create_task(self._health_check_loop())
        return self._task
    
    async def stop(self):
        """Stop the health check service."""
        logger.info("Stopping health check service")
        self._running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("Health check task cancelled")
        
        if self._client:
            await self._client.aclose()
        
        logger.info("Health check service stopped")
    
    async def _health_check_loop(self):
        """Main loop that periodically checks blocked nodes."""
        logger.info(f"Health check loop started (interval: {self.check_interval}s)")
        
        while self._running:
            try:
                # Only run health checks on master node
                if not cluster_manager.is_master:
                    logger.debug("Not master node, skipping health check")
                    await asyncio.sleep(self.check_interval)
                    continue
                
                blocked_nodes = self._get_blocked_nodes()
                
                if blocked_nodes:
                    logger.info(f"Found {len(blocked_nodes)} blocked nodes to check")
                    for node in blocked_nodes:
                        await self._check_and_recover_node(node)
                else:
                    logger.debug("No blocked nodes to check")
                
                await asyncio.sleep(self.check_interval)
                
            except asyncio.CancelledError:
                logger.info("Health check loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}", exc_info=True)
                await asyncio.sleep(self.check_interval)
    
    def _get_blocked_nodes(self) -> list[ClusterNode]:
        """Get list of currently blocked nodes."""
        return [node for node in cluster_manager.cluster_nodes if node.blocked]
    
    async def _check_and_recover_node(self, node: ClusterNode):
        """Check if a blocked node is healthy and recover it if possible."""
        try:
            logger.info(f"Checking blocked node: {node.url}")
            
            # Try to ping the node
            if not await self._ping_node(node):
                logger.warning(f"Node {node.url} did not respond to ping")
                return
            
            logger.info(f"Node {node.url} is responsive, starting catchup process")
            
            # Determine catchup timestamp
            # If node has never been updated, catch up from 1 hour ago
            catchup_from = node.last_update
            
            logger.info(f"Catching up node {node.url} from {catchup_from}")
            
            # Send batch writes to catch up the node
            if await cluster_manager.send_batch_writes(node.url, catchup_from):
                logger.info(f"Successfully caught up node {node.url}")
                # Unblock the node
                await cluster_manager.unblock_node(node.url)
                logger.info(f"Node {node.url} unblocked and ready")
            else:
                logger.warning(f"Failed to catch up node {node.url}, will retry on next check")
        
        except Exception as e:
            logger.error(f"Error checking and recovering node {node.url}: {e}", exc_info=True)
    
    async def _ping_node(self, node: ClusterNode) -> bool:
        """Ping a node to check if it's responsive."""
        try:
            logger.debug(f"Pinging node: {node.url}")
            response = await self._client.get(f"{node.url}/health")
            
            if response.status_code == 200:
                logger.debug(f"Node {node.url} ping successful")
                return True
            else:
                logger.debug(f"Node {node.url} ping failed with status: {response.status_code}")
                return False
                
        except Exception as e:
            logger.debug(f"Node {node.url} ping failed with error: {e}")
            return False

node_recovery_service = NodeRecoveryService()