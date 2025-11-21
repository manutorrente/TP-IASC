"""
Shard Strategy Module

Implements consistent hashing for entity distribution across shards.
Each entity type uses a deterministic hash function to assign to shards.
"""

import hashlib
from typing import Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class EntityType(str, Enum):
    """Business entity types that are sharded"""
    OPERATOR = "operator"
    USER = "user"
    WINDOW = "window"
    RESERVATION = "reservation"
    ALERT = "alert"
    NOTIFICATION = "notification"


class ShardStrategy:
    """
    Implements consistent hashing for shard assignment.
    
    Uses SHA-256 hash function for uniform distribution.
    Ensures same entity always maps to same shard.
    """
    
    def __init__(self, num_shards: int):
        """
        Initialize shard strategy.
        
        Args:
            num_shards: Total number of shards in the cluster
        """
        if num_shards <= 0:
            raise ValueError("Number of shards must be positive")
        
        self.num_shards = num_shards
        logger.info(f"ShardStrategy initialized with {num_shards} shards")
    
    def calculate_shard_id(self, entity_id: str) -> int:
        """
        Calculate shard ID for an entity using consistent hashing.
        
        Args:
            entity_id: Unique identifier for the entity
            
        Returns:
            Shard ID (0 to num_shards-1)
        """
        # Use SHA-256 for uniform distribution
        hash_value = hashlib.sha256(entity_id.encode()).hexdigest()
        hash_int = int(hash_value, 16)
        shard_id = hash_int % self.num_shards
        
        logger.debug(f"Entity {entity_id} mapped to shard {shard_id}")
        return shard_id
    
    def get_shard_key(self, entity_type: EntityType, entity_id: str) -> str:
        """
        Generate shard key for an entity.
        
        For most entities, the entity_id is the shard key.
        For related entities (e.g., alerts by user_id), use the parent key.
        
        Args:
            entity_type: Type of entity
            entity_id: Entity identifier
            
        Returns:
            Shard key string
        """
        # For alerts and notifications, we shard by user_id
        # The entity_id format is expected to be "user_id" for these types
        return entity_id
    
    def get_shard_range(self, shard_id: int) -> tuple[int, int]:
        """
        Get the hash range for a shard.
        
        Args:
            shard_id: Shard identifier
            
        Returns:
            Tuple of (start_hash, end_hash) for the shard
        """
        if shard_id < 0 or shard_id >= self.num_shards:
            raise ValueError(f"Invalid shard_id: {shard_id}")
        
        # Calculate hash space per shard
        max_hash = 2 ** 256  # SHA-256 produces 256-bit hashes
        range_size = max_hash // self.num_shards
        
        start_hash = shard_id * range_size
        end_hash = start_hash + range_size - 1
        
        return (start_hash, end_hash)


# Global shard strategy instance
shard_strategy: Optional[ShardStrategy] = None


def initialize_shard_strategy(num_shards: int) -> ShardStrategy:
    """Initialize global shard strategy."""
    global shard_strategy
    shard_strategy = ShardStrategy(num_shards)
    return shard_strategy


def get_shard_strategy() -> ShardStrategy:
    """Get global shard strategy instance."""
    if shard_strategy is None:
        raise RuntimeError("ShardStrategy not initialized. Call initialize_shard_strategy first.")
    return shard_strategy
