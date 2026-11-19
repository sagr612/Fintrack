# 💳 FinTrack: Distributed Event-Driven Microservices Backend

> A production-grade, event-driven financial management and automated bookkeeping backend built with **Spring Boot 3 (Java 21)**, **FastAPI (Python 3.12)**, **Google Gemini 2.5 Flash**, **Apache Kafka**, **PostgreSQL 16**, **Redis 7**, and **Kubernetes**. Engineered for high throughput, strict transactional consistency, zero dual-writes, and resilience against distributed system failure modes.

[![Java](https://img.shields.io/badge/Java-21-orange.svg?style=flat-square&logo=openjdk)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.5.5-brightgreen.svg?style=flat-square&logo=spring)](https://spring.io/projects/spring-boot)
[![Python](https://img.shields.io/badge/Python-3.12-blue.svg?style=flat-square&logo=python)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-teal.svg?style=flat-square&logo=fastapi)](https://fastapi.tiangolo.com/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.4-red.svg?style=flat-square&logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg?style=flat-square&logo=postgresql)](https://www.postgresql.org/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.36-326CE5.svg?style=flat-square&logo=kubernetes)](https://kubernetes.io/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D.svg?style=flat-square&logo=redis)](https://redis.io/)

---

## 🏛️ Master System Architecture

```mermaid
graph TB
    subgraph ClientPerimeter ["Client Layer and Edge Security"]
        Client(["Mobile App (React Native / Expo 54)"])
        Gateway["Spring Cloud Gateway (:8080)<br/>Non-blocking Netty Reverse Proxy<br/>Inbound Anti-Spoofing Header Cleansing<br/>Multi-Tier Redis Rate Limiting<br/>Resilience4j Circuit Breakers"]
    end

    subgraph ServiceRegistry ["Service Discovery Tier"]
        Eureka["Netflix Eureka Registry (:8761)<br/>Dynamic Heartbeat and Lease Management"]
    end

    subgraph MicroservicesTier ["Microservices Tier (Spring Boot 3 and FastAPI)"]
        AuthSvc["AuthService1 (:9898)<br/>OAuth2-style Token Issuer and STR<br/>Salted SHA-256 Hashed Tokens"]
        UserSvc["UserService (:9001)<br/>User Identity and Profiles<br/>Redisson Distributed Locks"]
        ExpenseSvc["ExpenseService (:9810)<br/>Financial Ledger and Budget Thresholds<br/>Autoscaled via HPA"]
        NotifSvc["NotificationService (:9500)<br/>In-App Feed and Reactive WebClient<br/>Expo Push Dispatcher"]
        AISvc["AI SMS Service (:8000)<br/>FastAPI and Gemini 2.5 Flash + Regex<br/>Deterministic SHA-256 Fingerprinting"]
    end

    subgraph MessagingBackbone ["Event Streaming and Resilience"]
        KafkaBroker[["Apache Kafka Broker (:9092)<br/>Topics: user.events | expense.create.request<br/>expense.created | budget-alerts | user.deleted"]]
        KafkaDLT[["Dead Letter Topics (*.DLT)<br/>Poison Pill Isolation via Raw String Consumers"]]
        ZooKeeper[["Apache ZooKeeper (:2181)"]]
    end

    subgraph PersistenceCluster ["Relational and Cache Storage"]
        Postgres[("PostgreSQL 16 (:5432)<br/>Dedicated Schemas: authservice | userservice<br/>expenseservice | notificationservice")]
        RedisStore[("Redis 7 (:6379)<br/>Distributed Locks and Rate Limit Quotas")]
    end

    Client -->|"HTTPS / REST"| Gateway
    Gateway <--> Eureka
    AuthSvc <--> Eureka
    UserSvc <--> Eureka
    ExpenseSvc <--> Eureka
    NotifSvc <--> Eureka
    AISvc <--> Eureka

    Gateway --> AuthSvc
    Gateway --> UserSvc
    Gateway --> ExpenseSvc
    Gateway --> NotifSvc
    Gateway --> AISvc

    AuthSvc --> Postgres
    UserSvc --> Postgres
    ExpenseSvc --> Postgres
    NotifSvc --> Postgres

    UserSvc --> RedisStore
    Gateway --> RedisStore

    AuthSvc --> KafkaBroker
    UserSvc --> KafkaBroker
    ExpenseSvc --> KafkaBroker
    AISvc --> KafkaBroker

    KafkaBroker -.-> KafkaDLT
    KafkaBroker --> ExpenseSvc
    KafkaBroker --> NotifSvc
    KafkaBroker --> AuthSvc
    KafkaBroker --- ZooKeeper
```

---

## 🗺️ Microservices Portfolio & Documentation

Each microservice is an independently deployable, isolated bounded context with its own database schema, domain models, integration test suite, and dedicated documentation:

| Microservice | Port | Framework / Tech | Primary Role | Documentation |
| :--- | :--- | :--- | :--- | :--- |
| **`gateway`** | `8080` | Spring Cloud Gateway, Netty, Redis, Resilience4j | Edge reverse proxy, anti-spoofing filter, Redis token bucket rate limiter, circuit breakers. | [gateway/README.md](file:///c:/Users/greatest/Work-dev/expense-tracker-full-app/backend/gateway/README.md) |
| **`AuthService1`** | `9898` | Spring Boot 3, Spring Security 6, PostgreSQL | OAuth2-style auth authority, single-use refresh token rotation (STR), SHA-256 hashing, anti-enumeration reset. | [AuthService1/README.md](file:///c:/Users/greatest/Work-dev/expense-tracker-full-app/backend/AuthService1/README.md) |
| **`userService`** | `9001` | Spring Boot 3, Redisson, PostgreSQL, Cloudinary | User identity lifecycle, Redisson distributed locking (`user:lock:{id}`), Cloudinary avatar media uploads. | [userService/README.md](file:///c:/Users/greatest/Work-dev/expense-tracker-full-app/backend/userService/README.md) |
| **`expenseService`** | `9810` | Spring Boot 3, PostgreSQL, Kafka, HPA | Core financial ledger, client idempotency (`externalId`), optimistic locking (`@Version`), budget threshold alerts. | [expenseService/README.md](file:///c:/Users/greatest/Work-dev/expense-tracker-full-app/backend/expenseService/README.md) |
| **`ai-sms-service`** | `8000` | Python 3.12, FastAPI, Gemini 2.5 Flash, Kafka | Dual-path hybrid classification (Gemini semantic parsing + regex amount validation), graceful degradation. | [ai-sms-service/README.md](file:///c:/Users/greatest/Work-dev/expense-tracker-full-app/backend/ai-sms-service/README.md) |
| **`notification-service`**| `9500` | Spring Boot 3, Spring WebFlux, PostgreSQL | Atomic consumer idempotency gate, non-blocking reactive Expo push dispatcher, stale token pruning. | [notification-service/README.md](file:///c:/Users/greatest/Work-dev/expense-tracker-full-app/backend/notification-service/README.md) |
| **`service-discovery`** | `8761` | Netflix Eureka Server | Dynamic service registration, heartbeat monitoring, and client-side load balancing. | — |
| **`k8s`** | — | Kubernetes 1.36, HPA, ConfigMaps, Secrets | Cloud-native orchestration, 12 manifests, tuned cold-start probes, multi-DB ConfigMap initialization. | [k8s/README.md](file:///c:/Users/greatest/Work-dev/expense-tracker-full-app/backend/k8s/README.md) |

---

## 🔄 End-to-End Distributed System Flows

### 1. Zero-Touch Financial SMS Ingestion to Ledger Pipeline

This pipeline illustrates how an incoming bank transaction SMS on a mobile device automatically transitions through hybrid AI reasoning, event streaming, ledger persistence, budget threshold analysis, and instant push notification without human intervention:

```mermaid
sequenceDiagram
    autonumber
    actor Bank as "Banking / UPI Gateway"
    participant Mobile as "FinTrack Mobile Client"
    participant Gateway as "API Gateway (:8080)"
    participant AISvc as "AI SMS Service (:8000)"
    participant Kafka as "Apache Kafka Broker"
    participant ExpenseSvc as "Expense Service (:9810)"
    participant NotifSvc as "Notification Service (:9500)"
    participant Expo as "Expo Push Server"

    Bank->>Mobile: SMS received ("Rs. 450 debited at Starbucks")
    Mobile->>Gateway: POST /api/v1/sms/classify { smsText }
    Gateway->>Gateway: Strip headers and Inject verified X-User-Id
    Gateway->>AISvc: Route to lb://AI-SERVICE
    
    par Dual-Path Hybrid Inference
        AISvc->>AISvc: Gemini 2.5 Flash classifies intent and category (FOOD)
    and
        AISvc->>AISvc: Regex extracts exact amount 450.00 and merchant Starbucks
    end

    AISvc->>AISvc: Generate externalId: sms-sha256(userId + text)
    AISvc->>Kafka: Publish to expense.create.request (key=userId)
    AISvc-->>Gateway: 200 OK (autoCreated true, externalId)
    Gateway-->>Mobile: Display pending confirmation snackbar

    Kafka->>ExpenseSvc: Ingest expense.create.request
    rect rgb(240, 248, 255)
        Note over ExpenseSvc: ACID Transaction Boundary
        ExpenseSvc->>ExpenseSvc: Verify idempotency by (userId, externalId)
        ExpenseSvc->>ExpenseSvc: Persist Expense entity (@Version optimistic lock)
        ExpenseSvc->>ExpenseSvc: Calculate monthly category budget consumption
        ExpenseSvc->>ExpenseSvc: Stage EXPENSE_CREATED and BUDGET_ALERT outbox events
    end

    ExpenseSvc->>Kafka: Outbox worker leases SKIP LOCKED and publishes budget-alerts
    Kafka->>NotifSvc: Ingest budget-alerts
    NotifSvc->>NotifSvc: Atomic idempotency gate (insertIfAbsent)
    NotifSvc->>NotifSvc: Persist in-app notification record
    NotifSvc->>Expo: Reactive WebClient POST /push/send (Exponential backoff)
    Expo->>Mobile: Remote Push Notification ("Warning: 80% Food Budget Reached!")
```

---

### 2. Cryptographic Single-Use Refresh Token Rotation (STR) Flow

Prevents token replay attacks, session hijacking, and double-spend race conditions:

```mermaid
sequenceDiagram
    autonumber
    participant Client as "Mobile Client (Axios Interceptor)"
    participant Gateway as "API Gateway"
    participant AuthSvc as "AuthService1 (:9898)"
    participant DB as "PostgreSQL (authservice)"

    Client->>Gateway: POST /api/v1/expenses (with expired access token)
    Gateway-->>Client: 401 Unauthorized
    Note over Client: Axios Interceptor activates mutex and queues pending requests
    Client->>Gateway: POST /auth/v1/refreshToken (refreshToken)
    Gateway->>AuthSvc: Forward to lb://AUTH-SERVICE
    
    AuthSvc->>AuthSvc: Compute SHA-256 hash of inbound token
    AuthSvc->>DB: Query token by hash and verify expiry
    alt Token Invalid or Already Revoked
        AuthSvc-->>Client: 401 Unauthorized (Trigger full logout)
    else Token Valid and Unused
        AuthSvc->>DB: Atomic delete old token hash
        AuthSvc->>AuthSvc: Generate new UUID access and refresh token pair
        AuthSvc->>DB: Save salted SHA-256 hash of new refresh token
        AuthSvc-->>Gateway: 200 OK (newAccessToken, newRefreshToken)
        Gateway-->>Client: 200 OK
        Note over Client: Interceptor drains queued requests with new Bearer token
        Client->>Gateway: Replay original POST /api/v1/expenses
    end
```

---

### 3. Dual-Write Elimination via Transactional Outbox (`SKIP LOCKED`)

Guarantees 100% reliable event delivery to Apache Kafka without distributed 2PC transactions:

```mermaid
sequenceDiagram
    autonumber
    participant App as "Service Business Logic"
    participant DB as "PostgreSQL"
    participant Worker as "OutboxPoller (Worker Pod)"
    participant Kafka as "Apache Kafka Broker"
    participant DLT as "Dead Letter Topic"

    rect rgb(240, 255, 240)
        Note over App,DB: Single ACID Database Transaction
        App->>DB: INSERT INTO domain_table (users / expenses)
        App->>DB: INSERT INTO outbox_events (status PENDING)
    end

    loop Every 500ms (Multiple Pods in Parallel)
        Worker->>DB: Claim batch via FOR UPDATE SKIP LOCKED
        Note over Worker,DB: Locks disjoint row batch without contention across pods
        Worker->>Kafka: Publish event (acks=all)
        alt Kafka Publish Success
            Worker->>DB: UPDATE outbox_events SET status PUBLISHED
        else Transient Kafka Failure
            Worker->>DB: Increment attempt_count and backoff next_retry_at
        else 10 Consecutive Failures Exhausted
            Worker->>DB: UPDATE outbox_events SET status DEAD and save error trace
        end
    end
```

---

### 4. GDPR-Compliant Cascading Account Deletion Flow

Illustrates how deleting a user in `userService` cascades asynchronously across the entire ecosystem without tight coupling:

```mermaid
sequenceDiagram
    autonumber
    actor Admin as "User / Admin"
    participant Gateway as "API Gateway"
    participant UserSvc as "UserService (:9001)"
    participant Redis as "Redis (Distributed Lock)"
    participant Kafka as "Apache Kafka"
    participant AuthSvc as "AuthService1 (:9898)"
    participant ExpenseSvc as "ExpenseService (:9810)"
    participant NotifSvc as "NotificationService (:9500)"

    Admin->>Gateway: DELETE /api/v1/users/{userId}
    Gateway->>UserSvc: lb://USER-SERVICE
    UserSvc->>Redis: Acquire lock user:lock:{userId} (5s timeout)
    UserSvc->>UserSvc: Execute delete and stage user.deleted Outbox event
    UserSvc->>Redis: Release lock
    UserSvc-->>Admin: 204 No Content

    UserSvc->>Kafka: Publish event to user.deleted
    
    par Cascade Purge Auth Credentials
        Kafka->>AuthSvc: Ingest user.deleted
        AuthSvc->>AuthSvc: Idempotent purge: refresh tokens, reset tokens, credentials
    and Cascade Purge Financial Ledger
        Kafka->>ExpenseSvc: Ingest user.deleted
        ExpenseSvc->>ExpenseSvc: Idempotent purge: all user transactions and monthly budgets
    and Cascade Purge Push Tokens and Notifications
        Kafka->>NotifSvc: Ingest user.deleted
        NotifSvc->>NotifSvc: Idempotent purge: in-app notifications and registered push tokens
    end
```

---

## 📨 Global Kafka Event Catalog

| Topic Name | Partition Key | Publisher Service | Consumer Services | Payload Schema / Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **`expense.create.request`** | `userId` | `ai-sms-service` | `expenseService` | Dispatches high-confidence AI-parsed SMS transaction records for automated ledger insertion. |
| **`expense.created`** | `userId` | `expenseService` | `notification-service` | Emitted when an expense is persisted; triggers in-app notification confirmation. |
| **`budget-alerts`** | `userId` | `expenseService` | `notification-service` | Emitted when spending crosses 50%, 80%, or 100% of a monthly category limit; dispatches Expo push alerts. |
| **`user.events`** | `userId` | `AuthService1` | `userService` | Broadcasts user registration (`USER_CREATED`) to provision downstream profile metadata. |
| **`user.deleted`** | `userId` | `userService` | `AuthService1`, `expenseService`, `notification-service` | Triggers atomic cascading cleanup across credentials, transactions, budgets, and push tokens. |
| **`*.DLT` Topics** | `null` | DeadLetterPublishingRecoverer | Dedicated DLT Consumers | Raw string poison pill isolation store; prevents head-of-line partition blocking. |

---

## 🛡️ Distributed Systems Patterns Matrix

| Architectural Pattern | Problem Solved | Key Technologies & Implementations |
| :--- | :--- | :--- |
| **Transactional Outbox** | Eliminates dual-write inconsistency between DB and Kafka | PostgreSQL `SELECT FOR UPDATE SKIP LOCKED`, exponential retry backoff, `DEAD` status transitions. |
| **Distributed Locking** | Eliminates concurrent race conditions on user profile mutations | Redisson Redis locks keyed on `user:lock:{id}` with strict Lock $\to$ DB Transaction $\to$ Unlock ordering. |
| **Single-Use Token Rotation** | Prevents refresh token replay and session hijack attacks | Salted SHA-256 database hashes, single-use token lifecycle, starter-pistol concurrency defense. |
| **Poison Pill Resiliency** | Prevents unparseable JSON from crashing Kafka partition loops | `ErrorHandlingDeserializer`, Spring `DeadLetterPublishingRecoverer`, dedicated `StringDeserializer` DLT consumer. |
| **Dual-Path Hybrid NLP** | Balances LLM semantic reasoning with exact monetary precision | Gemini 2.5 Flash for categorization + regex for rupee amounts, UPI VPAs, and graceful local fallback. |
| **Client Idempotency** | Prevents duplicate ledger entries on network retries | Client UUID `externalId` enforced via DB composite unique constraint `(user_id, external_id)`. |
| **Non-Blocking Push I/O** | Prevents third-party push API latencies from blocking worker pools | Spring WebFlux `Reactive WebClient` with jittered exponential retry and dead token auto-purging. |
| **Distributed Rate Limiting** | Defends against brute-force attacks and noisy neighbors | Redis token-bucket rate limiting via `UserKeyResolver` enforced at edge gateway Netty layer. |
| **Horizontal Pod Autoscaling** | Automatically absorbs month-end financial traffic spikes | Kubernetes HPA scaling `expense-service` (1–5 pods) based on 70% target CPU utilization. |

---

## 🚀 Quickstart & Local Orchestration

### Option A: Run via Docker Compose (Recommended for Local Dev)

The entire microservices platform, along with Kafka, ZooKeeper, PostgreSQL, Redis, Zipkin, Prometheus, and Grafana, can be launched with a single command:

```powershell
# 1. Navigate to backend directory
cd backend

# 2. Start all infrastructure and microservice containers
docker compose -f expense_compose.yml up --build -d

# 3. View streaming logs across services
docker compose -f expense_compose.yml logs -f
```

### Option B: Run on Local Kubernetes

Ensure Docker Desktop has **Kubernetes enabled**:

```powershell
cd backend

# 1. Build local container images
docker build -t expensetracker-service-discovery:latest ./service-discovery
docker build -t expensetracker-auth-service:latest ./AuthService1
docker build -t expensetracker-user-service:latest ./userService
docker build -t expensetracker-expense-service:latest ./expenseService
docker build -t expensetracker-notification-service:latest ./notification-service
docker build -t expensetracker-ai-sms-service:latest ./ai-sms-service
docker build -t expensetracker-gateway:latest ./gateway

# 2. Create cluster namespace & inject secrets dynamically
kubectl create namespace expense-tracker
kubectl create secret generic microservice-secrets --from-env-file=.env -n expense-tracker --dry-run=client -o yaml | kubectl apply -f -

# 3. Deploy all 12 manifests
kubectl apply -f k8s/

# 4. Monitor until all pods report 1/1 Running
kubectl get pods -n expense-tracker -w
```

---

## 🧭 Infrastructure & Observability Port Reference

| Service / Tool | Local Port | URL / Health Endpoint | Purpose |
| :--- | :--- | :--- | :--- |
| **Spring Cloud Gateway** | `8080` | `http://localhost:8080/actuator/health` | Public Edge Perimeter & Reverse Proxy |
| **Netflix Eureka Registry**| `8761` | `http://localhost:8761` | Service Registry Web Dashboard |
| **Zipkin Distributed Tracing**| `9411`| `http://localhost:9411/zipkin/` | End-to-End Distributed Trace Explorer |
| **PostgreSQL 16 Database** | `5432` | `localhost:5432` | Multi-Database Relational Storage |
| **Redis 7 Cache & Locks** | `6379` | `localhost:6379` | Distributed Locks & Rate Limit Quotas |
| **Apache Kafka Broker** | `9092` | `localhost:9092` | High-Throughput Event Streaming Cluster |
| **Apache ZooKeeper** | `2181` | `localhost:2181` | Kafka Cluster Coordination |
