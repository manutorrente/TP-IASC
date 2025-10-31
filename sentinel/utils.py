import httpx
import logging 

logger = logging.getLogger(__name__)

async def async_request(method: str, url: str, **kwargs):
    async with httpx.AsyncClient() as client:
        response = await client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    
def request_error_wrapper(func):
    """Decorator to handle request errors."""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except httpx.HTTPError as e:
            logger.error(f"HTTP error during request in callable {func.__name__}: {e}", exc_info=True)
    return wrapper

class AppMixin:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.url = f"http://{self.host}:{self.port}"
        
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, AppMixin):
            return NotImplemented
        return self.host == value.host and self.port == value.port
    
    def __hash__(self) -> int:
        return hash((self.host, self.port))
    
    def __str__(self) -> str:
        return self.url
