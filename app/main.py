from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from routers import operators, users, windows, reservations, cluster
from services.notification_service import notification_service
from services.window_service import window_service
from services.reservation_service import reservation_service
from services.node_recovery_service import node_recovery_service
from storage import storage
from cluster.cluster_manager import cluster_manager
from cluster.modification_history import modification_history
from config import settings
import asyncio
import sys
import uvicorn
import logging

# Import sharding components if enabled
if settings.enable_sharding:
    from cluster.shard_strategy import initialize_shard_strategy
    from cluster.shard_manager import initialize_shard_manager
    from cluster.sharded_storage import sharded_storage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

port = sys.argv[1] if len(sys.argv) > 1 else settings.node_port
settings.node_port = int(port)
logger.info(f"Configured node port: {settings.node_port}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application lifespan...")
    try:
        # Initialize sharding if enabled
        if settings.enable_sharding:
            logger.info("Initializing sharding components...")
            initialize_shard_strategy(settings.num_shards)
            
            self_url = f"http://{settings.node_host}:{settings.node_port}"
            shard_mgr = initialize_shard_manager(settings.num_shards, self_url)
            logger.info(f"Shard strategy and manager initialized with {settings.num_shards} shards")
            
            # Use sharded storage
            active_storage = sharded_storage
            logger.info("Using sharded storage")
        else:
            active_storage = storage
            logger.info("Using standard storage")
        
        logger.info("Initializing services with storage")
        notification_service.set_storage(active_storage)
        window_service.set_storage(active_storage)
        window_service.set_notification_service(notification_service)
        reservation_service.set_storage(active_storage)
        reservation_service.set_window_service(window_service)
        reservation_service.set_notification_service(notification_service)
        node_recovery_service.set_storage(active_storage)
        logger.info("Services initialized successfully")
        
        logger.info("Initializing cluster manager...")
        await cluster_manager.initialize()
        logger.info("Cluster manager initialized successfully")
        
        # Initialize shard manager with cluster nodes if sharding enabled
        if settings.enable_sharding:
            logger.info("Initializing shard assignments...")
            # Get cluster nodes from cluster manager
            cluster_nodes = [node.url for node in cluster_manager.cluster_nodes]
            cluster_nodes.append(cluster_manager.get_self_url())
            await shard_mgr.initialize(cluster_nodes)
            logger.info(f"Shard assignments initialized with {len(cluster_nodes)} nodes")
        
        logger.info("Starting background tasks...")
        notification_task = asyncio.create_task(notification_service.start())
        window_monitor_task = asyncio.create_task(window_service.monitor_windows())
        history_cleanup_task = asyncio.create_task(modification_history.start_cleanup_loop())
        node_recovery_task = asyncio.create_task(node_recovery_service.start())
        logger.info("Background tasks started successfully")
        
        yield
        
        logger.info("Shutting down background tasks...")
        notification_task.cancel()
        window_monitor_task.cancel()
        history_cleanup_task.cancel()
        node_recovery_task.cancel()
        
        try:
            await notification_task
            await window_monitor_task
            await history_cleanup_task
            await node_recovery_task
        except asyncio.CancelledError:
            logger.info("Background tasks cancelled successfully")
        
        logger.info("Shutting down cluster manager...")
        await cluster_manager.shutdown()
        
        # Shutdown shard manager if enabled
        if settings.enable_sharding:
            logger.info("Shutting down shard manager...")
            await shard_mgr.shutdown()
        
        logger.info("Application shutdown complete")
    except Exception as e:
        logger.error(f"Error during lifespan: {e}", exc_info=True)
        raise

app = FastAPI(title="SatLink API", lifespan=lifespan)

logger.info("Configuring CORS middleware")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger.info("Including routers")
app.include_router(operators.router, prefix="/operators", tags=["operators"])
logger.debug("Operators router included")
app.include_router(users.router, prefix="/users", tags=["users"])
logger.debug("Users router included")
app.include_router(windows.router, prefix="/windows", tags=["windows"])
logger.debug("Windows router included")
app.include_router(reservations.router, prefix="/reservations", tags=["reservations"])
logger.debug("Reservations router included")
app.include_router(cluster.router, prefix="/cluster", tags=["cluster"])
logger.debug("Cluster router included")

@app.get("/")
async def root():
    logger.debug("Root endpoint called")
    return {"message": "SatLink API - Satellite Resource Management"}

@app.get("/health")
async def health():
    logger.debug("Health check endpoint called")
    return {"status": "ok"}

if __name__ == "__main__":
    logger.info(f"Starting uvicorn server on {settings.node_host}:{settings.node_port}")
    uvicorn.run(app, host=settings.node_host, port=settings.node_port, reload=False)