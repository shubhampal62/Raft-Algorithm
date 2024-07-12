# Raft Consensus Algorithm with Leader Lease Modification

## Overview

This project implements the Raft consensus algorithm with a leader lease modification for faster read operations in distributed systems. The implementation focuses on creating a fault-tolerant key-value store that maintains consistency across multiple nodes while optimizing read performance.

## Features

- **Raft Consensus Algorithm**: Core implementation of the Raft protocol for distributed consensus.
- **Leader Lease Modification**: Optimized read operations using time-based leader leases.
- **Persistent Storage**: Durable storage of logs and metadata for crash recovery.
- **Client Interaction**: Support for SET and GET operations on the key-value store.
- **Fault Tolerance**: Handles node failures and leader elections seamlessly.

## System Architecture

- **Nodes**: Each node is a separate process hosted on a Google Cloud Virtual Machine.
- **Communication**: Uses gRPC (recommended) or ZeroMQ for inter-node and client-node communication.
- **Storage**: Persistent storage of logs and metadata in human-readable format.

## Key Components

1. **Leader Election**: Implements randomized election timeouts and vote request handling.
2. **Log Replication**: Ensures consistent log replication across nodes.
3. **Leader Lease**: Implements time-based leases for optimized read operations.
4. **Client Handling**: Manages client requests for SET and GET operations.
5. **Commit Management**: Handles the committing of entries on leader and follower nodes.

## Implementation Details

### Storage Structure

```
assignment/
├─ logs_node_0/
│  ├─ logs.txt
│  ├─ metadata.txt
│  ├─ dump.txt
├─ logs_node_1/
│  ├─ logs.txt
│  ├─ metadata.txt
│  ├─ dump.txt
```

### Supported Operations

- `SET K V`: Maps key K to value V.
- `GET K`: Retrieves the latest committed value of key K.
- `NO-OP`: Empty instruction for log consistency.

### RPCs

- `AppendEntry`: For log replication and heartbeats.
- `RequestVote`: For leader election process.

### Leader Lease Mechanism

- Lease duration: Fixed value between 2-10 seconds.
- Lease propagation: Through heartbeat mechanism.
- Lease renewal: At each heartbeat cycle.

## Testing Scenarios

1. Basic cluster startup and leader election
2. Log replication with SET and GET requests
3. Follower node failure and recovery
4. Leader node failure and new leader election
5. Majority follower failure and leader lease timeout
6. Complete cluster failure (except two followers)


For more detailed information about the Raft algorithm and the leader lease modification, please refer to the [Raft paper](https://raft.github.io/raft.pdf) and the assignment specifications.
