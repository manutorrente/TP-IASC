from functools import lru_cache
import logging
import httpx
from typing import Optional, TYPE_CHECKING
from dataclasses import dataclass
from typing import Optional, List
from enum import Enum
from collections import Counter

from utils import async_request, AppMixin
from .api import App

if TYPE_CHECKING:
    from services.peer_service import PeerService

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


class Cluster:
    def __init__(self, instances: Optional[list['AppInstance']] = None):
        self.instances = instances or []
        self.master: Optional['AppInstance'] = None
        self.total_shards: int = 0

    @lru_cache(maxsize=15)
    def get_instance(self, host: str, port: int) -> Optional['AppInstance']:
        for instance in self.instances:
            if instance.host == host and instance.port == port:
                return instance
        return None

    def _get_alive_instances(self) -> List['AppInstance']:
        """Get all instances that are currently up"""
        return [inst for inst in self.instances if inst.is_objectively_up]

    def _count_master_shards_per_instance(self) -> dict['AppInstance', int]:
        """Count how many master shards each alive instance has"""
        counts = {}
        for instance in self._get_alive_instances():
            master_count = sum(1 for s in instance.shards if s.role == ShardRole.MASTER)
            counts[instance] = master_count
        return counts

    def _get_instance_with_most_masters(self) -> Optional['AppInstance']:
        """Get the instance with the most master shards"""
        counts = self._count_master_shards_per_instance()
        if not counts:
            return None
        return max(counts, key=counts.get)

    def _get_instance_with_least_masters(self) -> Optional['AppInstance']:
        """Get the instance with the least master shards"""
        counts = self._count_master_shards_per_instance()
        if not counts:
            return None
        return min(counts, key=counts.get)

    def assign_shards(self, replication_factor: int = 2) -> None:
        """
        Dynamically assigns or reassigns shards based on cluster state.
        """
        logger.info("Assigning shards based on current cluster state")
        alive_instances = self._get_alive_instances()
        
        if not alive_instances:
            logger.warning("No alive instances available for shard assignment")
            return

        num_alive = len(alive_instances)

        # Initial setup
        if self.total_shards == 0:
            self.total_shards = num_alive
            logger.info(f"Initial shard assignment: creating {self.total_shards} shards")
            self._initial_shard_assignment(replication_factor)
            self._log_final_state()
            return

        # Check if we need to handle node changes
        self._handle_node_changes(replication_factor)
        self._log_final_state()

    def _initial_shard_assignment(self, replication_factor: int) -> None:
        """Initial shard assignment when cluster is first created"""
        alive_instances = self._get_alive_instances()
        
        for i, instance in enumerate(alive_instances):
            instance.shards = []
            # Assign master shard
            master_shard = Shard(shard_id=i, role=ShardRole.MASTER)
            instance.shards.append(master_shard)
            logger.info(f"Instance {instance.host}:{instance.port} assigned master shard {i}")

        # Assign replicas
        self._reassign_all_replicas(replication_factor)

    def _handle_node_changes(self, replication_factor: int) -> None:
        """Handle nodes being added, removed, or coming back online"""
        alive_instances = self._get_alive_instances()
        num_alive = len(alive_instances)

        # Find instances without master shards
        instances_without_master = [
            inst for inst in alive_instances
            if not any(s.role == ShardRole.MASTER for s in inst.shards)
        ]

        # Check if any existing masters belong to dead instances
        # FIXED: Check is_objectively_up instead of is_locally_up
        dead_master_shards = []
        for instance in self.instances:
            if not instance.is_objectively_up:
                for shard in instance.shards:
                    if shard.role == ShardRole.MASTER:
                        dead_master_shards.append(shard.shard_id)
                        logger.info(
                            f"Found dead master shard {shard.shard_id} on "
                            f"instance {instance.host}:{instance.port}"
                        )

        # Reassign dead master shards first
        if dead_master_shards:
            logger.info(f"Found {len(dead_master_shards)} master shards from dead instances")
            for shard_id in dead_master_shards:
                self._reassign_master_to_instance_with_least(shard_id, replication_factor)

        # Handle instances without masters
        for instance in instances_without_master:
            if self.total_shards >= num_alive:
                # Take a shard from an instance with multiple masters
                logger.info(
                    f"Node {instance.host}:{instance.port} added/returned. "
                    f"Shards ({self.total_shards}) >= nodes ({num_alive}), rebalancing..."
                )
                self._rebalance_shard_to_instance(instance, replication_factor)
            else:
                # Not enough shards, create a new one
                logger.info(
                    f"Node {instance.host}:{instance.port} added. "
                    f"Not enough shards ({self.total_shards} < {num_alive}), creating new shard..."
                )
                self._create_new_shard_for_instance(instance, replication_factor)

        # Final replica reassignment
        self._reassign_all_replicas(replication_factor)

    def _reassign_master_to_instance_with_least(
        self, shard_id: int, replication_factor: int
    ) -> None:
        """Reassign a master shard to the instance with the least masters"""
        target_instance = self._get_instance_with_least_masters()
        if not target_instance:
            logger.error(f"No alive instance available to reassign shard {shard_id}")
            return

        # Remove this shard from all instances first
        for instance in self.instances:
            instance.shards = [s for s in instance.shards if s.shard_id != shard_id]

        # Assign as master to target instance
        master_shard = Shard(shard_id=shard_id, role=ShardRole.MASTER)
        target_instance.shards.append(master_shard)
        
        logger.info(
            f"Reassigned master shard {shard_id} to {target_instance.host}:{target_instance.port} "
            f"(least masters)"
        )

    def _rebalance_shard_to_instance(
        self, target_instance: 'AppInstance', replication_factor: int
    ) -> None:
        """Take a master shard from instance with most masters and give it to target"""
        source_instance = self._get_instance_with_most_masters()
        if not source_instance:
            logger.error("No instance with master shards found for rebalancing")
            return

        # Count masters on source
        master_count = sum(1 for s in source_instance.shards if s.role == ShardRole.MASTER)
        
        if master_count <= 1:
            # Source only has 1 master, can't take it. Create new shard instead.
            logger.info(f"Source instance only has 1 master, creating new shard instead")
            self._create_new_shard_for_instance(target_instance, replication_factor)
            return

        # Find a master shard to transfer
        shard_to_transfer = None
        for shard in source_instance.shards:
            if shard.role == ShardRole.MASTER:
                shard_to_transfer = shard
                break

        if not shard_to_transfer:
            logger.error("No master shard found to transfer")
            return

        # Remove master from source, make it replica
        source_instance.shards.remove(shard_to_transfer)
        replica_shard = Shard(shard_id=shard_to_transfer.shard_id, role=ShardRole.REPLICA)
        source_instance.shards.append(replica_shard)

        # Check if target already has this shard as replica
        existing_shard = None
        for shard in target_instance.shards:
            if shard.shard_id == shard_to_transfer.shard_id:
                existing_shard = shard
                break

        if existing_shard:
            # Promote existing replica to master
            existing_shard.role = ShardRole.MASTER
        else:
            # Add as new master
            master_shard = Shard(shard_id=shard_to_transfer.shard_id, role=ShardRole.MASTER)
            target_instance.shards.append(master_shard)

        logger.info(
            f"Rebalanced shard {shard_to_transfer.shard_id} from "
            f"{source_instance.host}:{source_instance.port} to "
            f"{target_instance.host}:{target_instance.port}"
        )

    def _create_new_shard_for_instance(
        self, instance: 'AppInstance', replication_factor: int
    ) -> None:
        """Create a new shard and assign it as master to the given instance"""
        new_shard_id = self.total_shards
        self.total_shards += 1
        
        master_shard = Shard(shard_id=new_shard_id, role=ShardRole.MASTER)
        instance.shards.append(master_shard)
        
        logger.info(
            f"Created new shard {new_shard_id} and assigned as master to "
            f"{instance.host}:{instance.port}"
        )

    def _reassign_all_replicas(self, replication_factor: int) -> None:
        """Reassign replicas for all instances based on current topology"""
        alive_instances = self._get_alive_instances()
        
        if len(alive_instances) <= 1:
            # No replication possible with 1 or fewer instances
            return

        # Build a map of all current master locations
        master_to_instance = {}  # shard_id -> instance
        for instance in alive_instances:
            for shard in instance.shards:
                if shard.role == ShardRole.MASTER:
                    master_to_instance[shard.shard_id] = instance

        # Clear all replicas from alive instances, keep only masters
        for instance in alive_instances:
            instance.shards = [s for s in instance.shards if s.role == ShardRole.MASTER]

        # For each master shard, assign replicas using ring topology
        for shard_id, master_instance in master_to_instance.items():
            master_idx = alive_instances.index(master_instance)
            replicas_needed = min(replication_factor, len(alive_instances) - 1)
            
            logger.debug(
                f"Assigning {replicas_needed} replicas for shard {shard_id} "
                f"(master on {master_instance.host}:{master_instance.port})"
            )
            
            for offset in range(1, replicas_needed + 1):
                replica_idx = (master_idx + offset) % len(alive_instances)
                replica_instance = alive_instances[replica_idx]
                
                # Add replica if not already present
                if not any(s.shard_id == shard_id for s in replica_instance.shards):
                    replica_shard = Shard(shard_id=shard_id, role=ShardRole.REPLICA)
                    replica_instance.shards.append(replica_shard)
                    logger.debug(
                        f"Added replica of shard {shard_id} to "
                        f"{replica_instance.host}:{replica_instance.port}"
                    )

    def _log_final_state(self) -> None:
        """Log the final shard assignment for debugging"""
        logger.info("=== Final shard assignment ===")
        for instance in self.instances:
            masters = [s.shard_id for s in instance.shards if s.role == ShardRole.MASTER]
            replicas = [s.shard_id for s in instance.shards if s.role == ShardRole.REPLICA]
            logger.info(
                f"Instance {instance.host}:{instance.port} "
                f"(objectively_up={instance.is_objectively_up}, locally_up={instance.is_locally_up}): "
                f"masters={masters}, replicas={replicas}"
            )

    def set_master(self, host: str, port: int) -> None:
        master_instance = self.get_instance(host, port)
        if master_instance:
            self.master = master_instance
            logger.info(f"Set master instance to {host}:{port}")
        else:
            logger.warning(f"Instance {host}:{port} not found in cluster. Cannot set as master.")

    def add_instance(self, host: str, port: int) -> None:
        # Check if instance already exists
        existing = self.get_instance(host, port)
        if existing:
            logger.info(f"Instance {host}:{port} already exists in cluster")
            return

        instance = AppInstance(host, port)
        logger.info(f"Adding instance {host}:{port} to cluster")
        self.instances.append(instance)
        
        # Clear cache since instances changed
        self.get_instance.cache_clear()

    def remove_instance(self, host: str, port: int) -> None:
        """Remove an instance from the cluster"""
        instance = self.get_instance(host, port)
        if instance and instance in self.instances:
            logger.info(f"Removing instance {host}:{port} from cluster")
            self.instances.remove(instance)
            self.get_instance.cache_clear()
        else:
            logger.warning(f"Instance {host}:{port} not found in cluster")

    def choose_master_arbitrary(self) -> Optional["AppInstance"]:
        if not self.instances:
            return None
        self.master = self.instances[0]
        return self.master

    def add_instances(self, instances: list[App]) -> None:
        for inst in instances:
            self.add_instance(inst.host, inst.port)
        self.assign_shards()

    def get_instances_list(self) -> list[App]:
        return [App(host=instance.host, port=instance.port) for instance in self.instances]

    def get_instances(self) -> list['AppInstance']:
        return self.instances


def check_down_after(func):
    """Decorator to check objective down state after method execution."""
    async def wrapper(self, remote_peers: "PeerService"):
        logger.debug(f"Executing {func.__name__} for instance {self.host}:{self.port}")
        await func(self, remote_peers)
        await remote_peers.check_objectively_down(self)
    return wrapper


class AppInstance(AppMixin):
    def __init__(self, host: str, port: int):
        super().__init__(host, port)
        self.remote_down_counter = 0
        self.is_locally_up = True
        self.is_objectively_up = True
        self.state_down_counter = 0
        self.shards: List[Shard] = []

    def get_master_shards(self) -> List[Shard]:
        """Get all shards this instance is master of"""
        return [s for s in self.shards if s.role == ShardRole.MASTER]

    def get_replica_shards(self) -> List[Shard]:
        """Get all shards this instance has replicas of"""
        return [s for s in self.shards if s.role == ShardRole.REPLICA]

    async def get_health(self) -> str:
        url = f"{self.url}/health"
        try:
            response = await async_request("GET", url)
            status = response.get("status", "unknown")
        except httpx.HTTPError:
            status = "unreachable"
        return status

    async def is_healthy(self, peer_service: "PeerService") -> None:
        status = await self.get_health()
        logger.debug(f"Checking health for instance {self.host}:{self.port}. Status: {status}")
        
        if status == "ok" and self.state_down_counter > 0:
            await self.mark_local_up(peer_service)
            self.state_down_counter = 0
        elif status != "ok" and self.state_down_counter < 3:
            self.state_down_counter += 1
            logger.info(
                f"Instance {self.host}:{self.port} health check failed "
                f"{self.state_down_counter} times."
            )
        elif status != "ok" and self.state_down_counter == 3:
            self.state_down_counter += 1
            await self.mark_local_down(peer_service)
        elif status != "ok" and self.state_down_counter % 20 == 0:
            logger.info(
                f"Instance {self.host}:{self.port} health check failed "
                f"{self.state_down_counter} times."
            )
        else:
            logger.debug(f"Instance {self.host}:{self.port} is healthy.")

    @check_down_after
    async def add_remote_down(self, peer_service: "PeerService"):
        logger.info(f"Instance {self.host}:{self.port} reported as remotely down.")
        self.remote_down_counter += 1

    @check_down_after
    async def mark_local_down(self, peer_service: "PeerService"):
        logger.warning(f"Instance {self.host}:{self.port} marked as locally down.")
        self.is_locally_up = False
        await peer_service.add_local_down(self)

    @check_down_after
    async def mark_local_up(self, peer_service: "PeerService"):
        logger.info(f"Instance {self.host}:{self.port} marked as locally up.")
        self.is_locally_up = True
        await peer_service.remove_local_down(self)

    @check_down_after
    async def add_remote_up(self, peer_service: "PeerService"):
        logger.info(f"Instance {self.host}:{self.port} reported as remotely up.")
        if self.remote_down_counter > 0:
            self.remote_down_counter -= 1

    def down_count(self) -> int:
        return self.is_locally_up + self.remote_down_counter

    def state(self) -> dict:
        master_shards = self.get_master_shards()
        return {
            "is_locally_up": self.is_locally_up,
            "is_objectively_up": self.is_objectively_up,
            "remote_down_counter": self.remote_down_counter,
            "master_shards": [s.shard_id for s in master_shards],
            "replica_shards": [s.shard_id for s in self.get_replica_shards()],
        }