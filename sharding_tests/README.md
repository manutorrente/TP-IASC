# Sharding Test Suite

Comprehensive test suite for validating sharding implementation with master-slave replication and failover.

## Test Coverage

### Test Categories

1. **Shard Assignment (Tests 1-3)**
   - Consistent hashing validation
   - Entity distribution across shards
   - Replica assignment with N/2+1 quorum

2. **Sharded Operations (Tests 4-6)**
   - Write routing to correct shard master
   - Read load balancing across replicas
   - Cross-shard query aggregation

3. **Shard Failover (Tests 7-9)**
   - Master failure detection
   - New master election
   - Data consistency after failover

4. **Shard Replication (Tests 10-12)**
   - Write quorum enforcement
   - Replica synchronization
   - Concurrent writes to different shards

5. **Shard Recovery (Tests 13-14)**
   - Failed node recovery and rejoin
   - Shard rebalancing with new nodes

6. **Integration (Test 15)**
   - End-to-end workflow validation

## Running Tests

```bash
cd sharding_tests
pip install -r requirements.txt
pytest test_sharding.py -v
```

## Test Requirements

- Python 3.9+
- pytest with asyncio support
- httpx for async HTTP requests

## Architecture Validated

- **Sharding Strategy**: Consistent hashing with SHA-256
- **Replication**: N/2+1 replicas per shard (3 replicas for 5 nodes)
- **Failover**: Automatic master election on failure
- **Consistency**: Write quorum and replica synchronization
