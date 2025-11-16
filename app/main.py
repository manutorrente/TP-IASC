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
        logger.info("Initializing services with storage")
        notification_service.set_storage(storage)
        window_service.set_storage(storage)
        window_service.set_notification_service(notification_service)
        reservation_service.set_storage(storage)
        reservation_service.set_window_service(window_service)
        reservation_service.set_notification_service(notification_service)
        node_recovery_service.set_storage(storage)
        logger.info("Services initialized successfully")
        
        logger.info("Initializing cluster manager...")
        await cluster_manager.initialize()
        logger.info("Cluster manager initialized successfully")
        
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