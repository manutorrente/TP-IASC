from conf import sentinels
import httpx
import jump
import logging
import uuid


logger = logging.getLogger(__name__)


        
    

async def async_request(method: str, url: str, **kwargs) -> dict:
    logger.debug(f"Making {method} request to {url}")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, follow_redirects=True, **kwargs)
            response.raise_for_status()
            logger.debug(f"Request to {url} succeeded with status {response.status_code}")
            return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error for {method} {url}: {e.response.status_code} - {e.response.text}")
        raise
    except httpx.HTTPError as e:
        logger.error(f"Request error for {method} {url}: {str(e)}")
        raise


async def get_sentinel_coordinator() -> str | None:
    sentinel = sentinels[0]
    url = f"http://{sentinel}/peers"
    logger.info(f"Getting sentinel coordinator from {sentinel}")
    try:
        response = await async_request("GET", url)
        coordinator = response.get("coordinator")
        logger.info(f"Sentinel coordinator found: {coordinator}")
        return coordinator
    except httpx.HTTPError as e:
        logger.warning(f"Failed to get sentinel coordinator: {str(e)}")
        return None
    
async def get_master_for_shard(shard_id: int) -> tuple[str | None, int]:
    logger.info(f"Getting master for shard {shard_id}")
    coordinator = await get_sentinel_coordinator()
    if not coordinator:
        logger.error("No available sentinel coordinator")
        raise RuntimeError("No available sentinel coordinator")

    url = f"http://{coordinator}/shard-master"
    params = {"shard_id": shard_id}
    try:
        response = await async_request("GET", url, params=params)
        master_instance = response.get("node_address")
        num_shards = response.get("num_shards")
        if num_shards is None:
            logger.error("Invalid response from sentinel coordinator - no num_shards")
            raise RuntimeError("Invalid response from sentinel coordinator")
        logger.info(f"Master for shard {shard_id}: {master_instance}, total shards: {num_shards}")
        return master_instance, num_shards
    except httpx.HTTPError as e:
        logger.error(f"Failed to get master for shard {shard_id}: {str(e)}")
        raise RuntimeError("Failed to get master for shard from sentinel coordinator")
    
async def get_slaves_for_shard(shard_id: int) -> list[str]:
    logger.info(f"Getting slaves for shard {shard_id}")
    coordinator = await get_sentinel_coordinator()
    if not coordinator:
        logger.error("No available sentinel coordinator")
        raise RuntimeError("No available sentinel coordinator")

    url = f"http://{coordinator}/shard-slaves"
    params = {"shard_id": shard_id}
    try:
        response = await async_request("GET", url, params=params)
        slave_instances = response.get("slave_addresses", [])
        logger.info(f"Slaves for shard {shard_id}: {slave_instances}")
        return slave_instances
    except httpx.HTTPError as e:
        logger.error(f"Failed to get slaves for shard {shard_id}: {str(e)}")
        raise RuntimeError("Failed to get slaves for shard from sentinel coordinator")

def uuid_to_int(uuid_str: str) -> int:
    """Convert UUID string to integer for jump hash."""
    hex_str = uuid_str.replace('-', '')
    return int(hex_str, 16)


master_cache: dict[int, str] = {}
slave_cache: dict[int, list[str]] = {}
num_shards: int = 0

def create_uuid_for_shard(other_id: str) -> str:
    global num_shards
    shard_id = jump.hash(uuid_to_int(other_id), num_shards)
    while True:
        new_uuid = str(uuid.uuid4())
        shard_assignment = jump.hash(uuid_to_int(new_uuid), num_shards)
        if shard_assignment == shard_id:
            return new_uuid    


async def forward_request_to_master(entity_id: str, method: str, endpoint: str, **kwargs) -> dict:
    global num_shards
    global master_cache
    global slave_cache
    
    logger.debug(f"Forwarding request for entity {entity_id}: {method} {endpoint}")
    
    if num_shards == 0:
        logger.info("First request - initializing num_shards")
        _, nshards = await get_master_for_shard(0)
        num_shards = nshards
        logger.info(f"Initialized num_shards to {num_shards}")
        
    shard_id = jump.hash(uuid_to_int(entity_id), num_shards)
    logger.debug(f"Entity {entity_id} mapped to shard {shard_id}")
    
    is_read_operation = method.upper() == "GET"
    
    # Try master first (from cache or fetch)
    if shard_id in master_cache:
        master_instance = master_cache[shard_id]
        url = f"{master_instance}{endpoint}"
        logger.debug(f"Using cached master {master_instance} for shard {shard_id}")
        
        try:
            response = await async_request(method, url, **kwargs)
            logger.info(f"Successfully forwarded {method} {endpoint} to shard {shard_id}")
            return response
        except httpx.HTTPError as e:
            logger.warning(f"Cached master {master_instance} failed: {str(e)}")
            del master_cache[shard_id]
            
            # For read operations, try slaves before giving up
            if is_read_operation:
                logger.info(f"Attempting to read from slave nodes for shard {shard_id}")
                try:
                    return await _try_read_from_slaves(shard_id, endpoint, **kwargs)
                except Exception as slave_error:
                    logger.error(f"All slave reads failed for shard {shard_id}: {str(slave_error)}")
            
            # If not a read operation or slaves failed, retry with fresh master lookup
            return await forward_request_to_master(entity_id, method, endpoint, **kwargs)
    else:
        logger.debug(f"Cache miss for shard {shard_id}, fetching master")
        master_instance, nshards = await get_master_for_shard(shard_id)
        
        if num_shards != nshards:
            logger.warning(f"Shard count changed from {num_shards} to {nshards}, recalculating shard assignment")
            num_shards = nshards
            master_cache.clear()
            slave_cache.clear()
            return await forward_request_to_master(entity_id, method, endpoint, **kwargs)
        
        if master_instance is None:
            logger.error(f"No master instance found for shard {shard_id}")
            
            # For read operations, try slaves as fallback
            if is_read_operation:
                logger.info(f"No master available, attempting to read from slave nodes for shard {shard_id}")
                try:
                    return await _try_read_from_slaves(shard_id, endpoint, **kwargs)
                except Exception as slave_error:
                    logger.error(f"All slave reads failed for shard {shard_id}: {str(slave_error)}")
            
            raise RuntimeError("No master instance found for shard")
        
        logger.info(f"Caching master {master_instance} for shard {shard_id}")
        master_cache[shard_id] = master_instance
        url = f"{master_instance}{endpoint}"
        
        try:
            response = await async_request(method, url, **kwargs)
            logger.info(f"Successfully forwarded {method} {endpoint} to shard {shard_id}")
            return response
        except httpx.HTTPError as e:
            logger.warning(f"Master {master_instance} failed: {str(e)}")
            del master_cache[shard_id]
            
            # For read operations, try slaves
            if is_read_operation:
                logger.info(f"Master failed, attempting to read from slave nodes for shard {shard_id}")
                try:
                    return await _try_read_from_slaves(shard_id, endpoint, **kwargs)
                except Exception as slave_error:
                    logger.error(f"All slave reads failed for shard {shard_id}: {str(slave_error)}")
            
            raise


async def _try_read_from_slaves(shard_id: int, endpoint: str, **kwargs) -> dict:
    """Try to read from slave nodes for a given shard."""
    global slave_cache
    
    # Get slaves from cache or fetch them
    if shard_id in slave_cache:
        slave_instances = slave_cache[shard_id]
        logger.debug(f"Using cached slaves for shard {shard_id}: {slave_instances}")
    else:
        logger.debug(f"Fetching slaves for shard {shard_id}")
        slave_instances = await get_slaves_for_shard(shard_id)
        if slave_instances:
            slave_cache[shard_id] = slave_instances
            logger.info(f"Cached {len(slave_instances)} slaves for shard {shard_id}")
    
    if not slave_instances:
        logger.error(f"No slave instances available for shard {shard_id}")
        raise RuntimeError(f"No slave instances available for shard {shard_id}")
    
    # Try each slave in sequence
    for i, slave_instance in enumerate(slave_instances):
        url = f"{slave_instance}{endpoint}"
        logger.debug(f"Trying slave {i+1}/{len(slave_instances)}: {slave_instance}")
        
        try:
            response = await async_request("GET", url, **kwargs)
            logger.info(f"Successfully read from slave {slave_instance} for shard {shard_id}")
            return response
        except httpx.HTTPError as e:
            logger.warning(f"Slave {slave_instance} failed: {str(e)}")
            continue
    
    # If all slaves failed, invalidate cache and raise
    if shard_id in slave_cache:
        del slave_cache[shard_id]
    raise RuntimeError(f"All slave reads failed for shard {shard_id}")