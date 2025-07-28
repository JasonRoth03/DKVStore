# Distributed Key-Value Store

This project is a fault-tolerant, distributed key-value store built in Java from the ground up. It demonstrates several core concepts of distributed systems to create a scalable and resilient data storage solution. The system is designed to run as a cluster of server nodes that coordinate to partition, replicate, and manage data.

## Core Features

* **Distributed & Scalable**: Data is automatically partitioned across a dynamic cluster of nodes using a **consistent hashing** ring. This algorithm minimizes data reshuffling when nodes join or leave the cluster, making it highly scalable.
* **Fault-Tolerant Service Discovery**: The system uses **Apache ZooKeeper** for robust, real-time service discovery. Each server registers itself as an ephemeral node, allowing for automatic failure detection if a node crashes or disconnects.
* **Coordinated Startup**: To prevent startup race conditions, the cluster uses a ZooKeeper-based **distributed barrier**, ensuring that no server begins processing requests until all expected nodes have registered.
* **Data Replication & Durability**: To prevent data loss, all writes are replicated across multiple nodes (the current replication factor is 3). This provides redundancy and high availability.
* **Strong Consistency via Quorum**: Write operations (`PUT`/`DELETE`) are confirmed using a **quorum-based** strategy. An operation is only considered successful after a majority of replicas (e.g., 2 out of 3) have acknowledged the write, guaranteeing data durability even in the event of a primary node failure.
* **Custom Networking Protocol**: All inter-node communication for request forwarding, replication, and acknowledgments is handled through a custom, length-prefixed TCP messaging protocol.

## How to Run

### Prerequisites

* Java 21 or higher
* Apache Maven
* Docker Desktop (for running ZooKeeper)

### Step 1: Start ZooKeeper

This project requires a running Apache ZooKeeper instance for coordination. The easiest way to start one is with Docker. Open your terminal and run:

```bash
docker run --name my-zookeeper -p 2181:2181 -d zookeeper:3.8
```
### Step 2: Build the Project

Navigate to the project's root directory (where `pom.xml` is located) and use Maven to compile the project and build the necessary artifacts.

```bash
mvn clean install
```
### Step 3: Run the Cluster

The `com.JasonRoth.Runner` class will start a local cluster of server instances. You can run this from your IDE or using Maven from the command line.

```bash
# To run from the command line using Maven
mvn exec:java -Dexec.mainClass="com.JasonRoth.Runner"
```
The runner will start 5 servers on HTTP ports `8000`, `8010`, `8020`, `8030`, and `8040`. Each server also uses an internal TCP port for peer communication (e.g., the server on `8000` uses `8002` for TCP).

## API Endpoints
You can send requests to any node in the cluster. The node will act as a coordinator and automatically forward the request to the correct primary node if necessary.

`POST /put`

Stores a key-value pair.

**Request Body:**
```bash
{
  "key": "my-key",
  "value": "my-value"
}
```
**Example using cURL:**
```bash
curl -X POST -H "Content-Type: application/json" -d '{"key":"hello", "value":"world"}' http://localhost:8000/put
```
`GET /get`

Retrieves the value for a given key.

**URL Parameter:** `key`

**Example using cURL:**
```bash
curl "http://localhost:8010/get?key=hello"
```
`DELETE /delete`

Deletes a key-value pair.

**URL Parameter:** `key`

**Example using cURL:**
```bash
curl -X DELETE "http://localhost:8020/delete?key=hello"
```
