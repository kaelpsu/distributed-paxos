# Distributed Paxos System

A distributed application implementing the Paxos consensus algorithm for state replication. The architecture is divided into three specialized node types and supports communication via TCP, HTTP, UDP, and gRPC.

## Architecture

*   **API Gateway (`gateway`)**: The entry point for external clients. It routes incoming requests to available business nodes and handles synchronous waiting via completable futures.
*   **Business Node (`biz`)**: Stateless nodes responsible for business logic. They discover active database nodes via heartbeats and route transactions to the Paxos consensus cluster.
*   **Database Node (`db`)**: Stateful nodes containing the Paxos Engine. They replicate state across the cluster and maintain the in-memory database.

## Prerequisites

*   Java 25
*   Maven

## Build

To compile the project and build the executable JAR file, run the following command in the project root:

```bash
mvn clean package
```

## Configuration

The network relies on a JSON configuration file to map node IDs to their IP addresses. Create a `topology.json` file in the same directory as your executable:

```json
{
  "gateway-1": "127.0.0.1",
  "biz-1": "127.0.0.1",
  "db-1": "127.0.0.1",
  "db-2": "127.0.0.1",
  "db-3": "127.0.0.1"
}

```

*Note: The application uses hard-coded ports: `8080` (TCP/HTTP), `9090` (UDP), and `50051` (gRPC).*

## Run

To start a node, execute the compiled JAR file and provide the node type, its unique ID, and the topology configuration file.

**Database Node:**

```bash
java -jar target/app.jar --type db --id db-1 --config topology.json

```

**Business Node:**

```bash
java -jar target/app.jar --type biz --id biz-1 --config topology.json

```

**API Gateway:**

```bash
java -jar target/app.jar --type gateway --id gateway-1 --config topology.json

```
