# Cross-Entity Write Solution for select-resource Endpoint

## Problem

The `select-resource` endpoint needs to modify two different entities:
- **Reservation**: Update status and selected resource
- **Window**: Mark resource as unavailable

These entities may have different masters in the cluster due to sharding. Previously, the code attempted to update both locally and replicate them, but there's no guarantee both updates would go to the same master.

### Critical Issue with Locks

If Node A acquired the window resource lock and then tried to forward the write to Node B (which is the window's master), Node B could receive a `prepare` request from Node A while Node A still held the lock. This would cause Node B's `prepare` request (sent back through the replication protocol) to deadlock since Node A wouldn't process it until releasing the lock.

## Solution Architecture

The solution implements a two-phase approach:

### 1. Local Update Phase (Lock Held)
- Node performs resource selection logic while holding the window resource lock
- Updates the entity it is master for locally
- Both entities have their data modified in memory but only local master update completes via replication

### 2. Remote Forward Phase (Lock Released)
- **CRITICAL**: Release the window resource lock BEFORE forwarding writes
- Forward writes to remote masters that don't have this node as their master
- Use a new `/cluster/forward-write` endpoint for cross-master coordination
- Wait for successful responses from each remote master

### Key Design Decision: Lock Release Point

```
Lock Acquired
    └─> Check resources
    └─> Update local master entity (if applicable)
    └─> Update remote master entity (if applicable)
Release Lock  ← CRITICAL POINT
    └─> Forward reservation to its master (if remote)
    └─> Forward window to its master (if remote)
```

This prevents deadlocks because:
- Remote masters can send prepare/commit requests without being blocked by the local lock
- Local updates already completed and don't need additional coordination
- Remote forwards happen after locks are released, allowing normal replication protocol flow

## Implementation Details

### New Method: `cluster_manager._send_write_to_master()`

Located in `app/cluster/cluster_manager.py`

```python
async def _send_write_to_master(self, entity_id: str, entity_type: str, operation: str, data: dict) -> bool
```

**Purpose**: Forward a write operation to the node that is the master for the given entity.

**When to use**: 
- Called from nodes that are NOT the master for the entity
- Used when a replica needs to write to an entity whose master is different from the current node

**Returns**: `True` if write was successfully replicated and committed, `False` otherwise

**Process**:
1. Determine which shard the entity belongs to
2. Find the master node for that shard
3. Send POST request to `/cluster/forward-write` endpoint on the master
4. Wait for response indicating successful replication to quorum

### New Endpoint: `/cluster/forward-write`

Located in `app/routers/cluster.py`

```python
@router.post("/forward-write")
async def forward_write(request: PrepareRequest, ...)
```

**Purpose**: Handle write requests forwarded from other nodes, when this node is the master.

**Flow**:
1. Verify this node is the master for the entity
2. Deserialize the request data to appropriate Pydantic model
3. Call `cluster_manager.replicate_write()` to perform full replication to replicas
4. Return success/failure

**Key Point**: This endpoint uses the normal replication flow, ensuring quorum consistency for the remote write.

### Updated Method: `reservation_service.select_resource()`

Located in `app/services/reservation_service.py`

**Key Changes**:

1. **Determine Master Nodes**:
   ```python
   window_is_local_master = cluster_manager._is_master_for_entity(window.id)
   reservation_is_local_master = cluster_manager._is_master_for_entity(reservation_id)
   ```

2. **Case 1: Both Local Masters**
   - Traditional flow: update both entities using normal replication
   - No lock release issues since both stay within this node's replication protocol

3. **Case 2: Different Masters**
   - Update local master entity while holding lock
   - Release lock BEFORE forwarding to remote masters
   - Forward each remote write and wait for success
   - If either forward fails, raise exception

**Lock Management**:
```python
async with resource_lock:
    # Make changes to window and reservation
    # Update local master entities
    logger.debug(f"Releasing lock for window {window.id} before forwarding to remote masters")
# Lock is now released

# Now forward to remote masters
if not reservation_is_local_master:
    await cluster_manager._send_write_to_master(...)
if not window_is_local_master:
    await cluster_manager._send_write_to_master(...)
```

## Consistency Guarantees

### Write Consistency
- Each entity is guaranteed to reach its master with quorum support
- Both local and remote writes use the 2-phase commit protocol

### Atomicity Considerations
- **Local update + Remote forward are NOT atomic**
- If local update succeeds but remote forward fails, local entity is updated but remote is not
- Client receives error and should retry, triggering compensating actions

### Deadlock Prevention
- Lock release before remote coordination prevents circular wait
- Prepare/commit requests can flow freely without blocking local locks

## Error Handling

### Failure Scenarios

1. **Local Update Fails**:
   - Error raised, method exits
   - No remote forwards sent
   - Client receives 500 error

2. **Remote Forward Fails** (after local update succeeded):
   - Error raised
   - Client receives 400 or 500 error
   - Inconsistency exists: local master updated, remote master not
   - **Application should implement compensating transaction or retry logic**

3. **Timeout on Forward**:
   - `_send_write_to_master()` includes timeout handling
   - Returns False on timeout
   - Client receives error and can retry

## Usage Example

```python
# Client calls select-resource endpoint
POST /reservations/{reservation_id}/select-resource
{
    "resource_type": "optical_channel"
}

# Node A (reservation master) receives request
# Node A checks: Window X is on Node B (different master)
# Node A:
#   - Acquires window resource lock
#   - Updates its local copy of reservation
#   - Sends prepare + commit for reservation to replicas
#   - Releases lock
#   - Sends forward-write to Node B for window update
# 
# Node B (window master) receives forward-write
# Node B:
#   - Receives prepare from Node A
#   - Responds with prepared status
#   - Receives commit, commits window update to its replicas
```

## Testing Recommendations

1. **Single Master Scenario**: Both entities have same master
   - Verify traditional replication flow still works
   - Ensure no performance regression

2. **Multi-Master Scenario**: Entities have different masters
   - Create reservation and window on different master nodes
   - Call select-resource from either node
   - Verify both updates reach correct masters

3. **Failure Scenarios**:
   - Remote master unreachable during forward → error returned
   - Local update succeeds but forward fails → client retries
   - Network partition between masters → handled by existing quorum logic

4. **Deadlock Testing**:
   - Concurrent select-resource calls on same window
   - Verify no deadlocks occur with multiple clients
   - Monitor lock wait times

## Future Improvements

1. **Distributed Transactions**: Implement 2-phase commit across both entities for true atomicity
2. **Compensation Logic**: Add compensating transactions to handle partial failures
3. **Retry Mechanism**: Add exponential backoff retry logic for transient failures
4. **Monitoring**: Add metrics for cross-master write latency and success rates
