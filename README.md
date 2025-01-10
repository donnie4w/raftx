# **RaftX**

RaftX is an extension of the classic Raft protocol, integrating the advantages of Multi-Paxos, ZAB (Zookeeper Atomic Broadcast), and Raft protocols. RaftX features rapid election, concurrent proposals, data synchronization, data rollback, and volatile data synchronization, making it suitable for high-concurrency and large-scale distributed system scenarios.

---

## **Table of Contents**

1. [Introduction](#introduction)
2. [Feature Overview](#feature-overview)
3. [System Architecture](#system-architecture)
4. [Installation Guide](#installation-guide)
5. [Usage](#usage)
6. [Application Scenarios](#application-scenarios)
7. [License](#license)

---

## **Introduction**

In distributed systems, consensus algorithms are typically used to coordinate the state among multiple nodes to ensure data consistency. The traditional **Raft** protocol is easy to understand but has certain limitations in scenarios with high concurrency, many nodes, and large-scale data synchronization. Therefore, RaftX extends Raft by incorporating the concurrent proposal capability of **Multi-Paxos** and the data synchronization feature of **ZAB**, providing a more efficient and stable distributed consensus solution.

[raftx wiki](https://tlnet.top/wiki/raftx)
---

## **Feature Overview**

### 1. **Rapid Election Mechanism**

- Elections are based on the majority rule principle of Raft and have been extended to ensure that a Leader is elected within a term.
- Avoids the multi-round re-election issue caused by circular voting in Raft.
- In clusters with usually 3-15 nodes, elections do not exceed one second.
- Even in clusters with dozens of nodes, elections can be completed within **0-2 seconds** under normal circumstances, with the main time spent on message notifications between nodes.

### 2. **Concurrent Proposals and Log Synchronization**

- Similar to **Multi-Paxos**, it supports concurrent handling of multiple proposals, allowing unordered log and transaction submissions to enhance concurrency processing capabilities, suitable for high-concurrency scenarios.

### 3. **Data Synchronization and Rollback**

- Like the data synchronization feature of **ZAB**, both Leaders and Followers automatically detect and synchronize missing data.
- Automatically rolls back data during cluster failures or failed proposals to maintain data consistency across nodes.

### 4. **Support for Distributed Volatile Data**
##### Volatile data refers to data that does not require persistent storage, especially data that no longer needs to be retained after quick expiration. This type of data is characterized by a short lifecycle, frequent updates, and can sometimes tolerate loss.
##### The volatile data feature in RaftX is not part of the protocol itself; it's an extension of RaftX primarily relying on its network architecture and partial synchronization protocol. It does not have persistent data logs, so unlike RaftX clusters, it cannot fully synchronize all data at any time when new nodes are added. It depends on the length of the log cache pool, which is currently set to 1048576, recording the latest 1048576 data logs. If a node reconnects after disconnection, as long as the number of unsynchronized data entries is less than 1048576, they can be synchronized back. Otherwise, only the latest 1048576 data entries will be synchronized if exceeded.
##### Since volatile data is not persisted, it offers very high efficiency, playing a crucial role in the timeliness of cluster systems.
##### Acquiring volatile data is similar to RaftX, where it can be obtained from the cluster or locally. When fetched from the cluster, it ensures strong consistency results. However, fetching from local provides eventual consistency, potentially retrieving dirty data, with efficiency comparable to accessing local cache. The choice of how to acquire data depends on the characteristics and requirements of specific business needs.
##### Volatile data plays a significant role in collaborative cluster operations and can be applied to numerous distributed services, such as distributed locks, distributed logging systems, and distributed messaging notifications.
#### Characteristics of Volatile Data
- Efficient Data Collaboration
   - Designed for fast read/write and high-concurrency scenarios, volatile data optimizes memory access efficiency to support substantial real-time interaction demands.
- Ordered Operations
   - Ensures that create, update, and delete operations occur in the same order on each node, which is critical for maintaining global view consistency and correctness in a distributed environment.
- TTL (time to live) for Data in Memory
   - Each volatile data item can have a set TTL, after which it expires and is removed from memory automatically.
- Event Listening and Triggering
   - Supports event listening and triggering for data operations like create, update, and delete.
- Strong Consistency
   - Ensures strong consistency for data reads and writes.

---

## **System Architecture**

```
+-----------------------------------------+
|                Raftx Api                |
+-----------------------------------------+
           |                 |
           v                 v
+-------------------+   +-------------------+
|       Node 1      |   |       Node 2      |
| (Leader/Follower) |   | (Leader/Follower) |
+-------------------+   +-------------------+
           |                 |
           v                 v
+-------------------+   +-------------------+
|       Node 3      |   |       Node 4      |
| (Leader/Follower) |   | (Leader/Follower) |
+-------------------+   +-------------------+
```

### **Component Description**

1. **Leader**: Responsible for receiving client requests, proposing and synchronizing logs.
2. **Follower**: Executes leader's data proposals, data commits, rollbacks, proxies client requests, etc.
3. **Raftx Api**: Sends requests to the cluster, retrieves data, or submits state updates.

---

## **Installation Guide**

### **Dependencies**

- **Operating System**: Linux, macOS, Windows
- **Programming Language**: Go 1.22 or higher
- **Dependent Tools**: Git

### **Project Installation**

```bash
go get https://github.com/donnie4w/raftx
```

### **API Call Example**

```go
config := &Config{ListenAddr: ":20001", PeerAddr: []string{"localhost:20001","localhost:20002","localhost:20003"}}
raft := NewRaftx(config)
raft.Open()
```

---

## **Application Scenarios**

1. **High-Concurrency Distributed Storage Systems**
   - Suitable for databases, KV stores, and other systems requiring rapid proposals and concurrent log synchronization.

2. **Real-Time Status Synchronization Systems**
   - For quickly expiring volatile data synchronization, such as user online status, location information, etc.

3. **Large-Scale Cluster Elections**
   - Fits distributed systems needing rapid Leader election.

4. **URL Systems and CDN**
   - Quickly shares volatile data between clusters to improve system response speed and availability.

---

## **License**

RaftX uses the [Apache-2.0 license](https://github.com/donnie4w/raftx?tab=Apache-2.0-1-ov-file#readme).