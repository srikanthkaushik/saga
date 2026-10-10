# SAGA: an order saga on Kafka

When a customer places an order here, four independent services must agree: **order-service** records it, **payment-service** charges the customer, **inventory-service** reserves stock and **shipping-service** schedules the delivery. Each service has its own database, so no single database transaction can cover them all. Instead the order runs as a **saga**: a sequence of local transactions coordinated by messages. If a later step fails, earlier steps are undone in reverse order by **compensations** (a reservation released, then a charge refunded). Once a shipment is scheduled the order stands: that step is the saga's **pivot**. A fifth service, **notification-service**, tells the customer the outcome; it listens to the order's events and never affects it.

This project is a working, tested implementation of that pattern, including the failure handling a real system needs:

- **Transactional outbox:** no message is lost or sent for a rolled-back change.
- **Idempotent consumers:** duplicates are harmless.
- **Retries with backoff and dead-letter topics,** with replay.
- **Timeouts:** a participant that never answers doesn't hang an order.
- **Fencing:** late messages can't charge, reserve or ship for an order that was already cancelled.
- **Compensatable, pivot and retriable steps:** payment and stock are undone in reverse order when shipping fails; notifications after the pivot only retry.
- **Orchestration and choreography side by side:** the orchestrator drives the saga with commands; notification-service reacts to domain events nobody waits for.
- **Alerting and manual resolution** for compensations that keep failing.
- **OAuth2/JWT security with Keycloak**: roles for viewers, operators and admins, and audit records of who did what.
- **A browser console** that shows each saga live and can call every endpoint.

Java 21 · Spring Boot 4.0.6 · PostgreSQL 17 · Kafka 4.0 · Maven multi-module · Windows CMD

## The picture

```mermaid
flowchart LR
    client([Browser / curl]) -->|POST /orders| order
    console["saga-ui console :8080"] -.->|proxy| order & payment & inventory & shipping & notification
    subgraph os["order-service :8081"]
        order[Orchestrator]
    end
    subgraph ps["payment-service :8082"]
        payment[Payment handler]
    end
    subgraph is["inventory-service :8083"]
        inventory[Inventory handler]
    end
    subgraph ss["shipping-service :8084"]
        shipping[Shipping handler]
    end
    subgraph ns["notification-service :8085"]
        notification[Notification handler]
    end
    order -->|payment.commands| payment
    order -->|inventory.commands| inventory
    order -->|shipping.commands| shipping
    payment -->|order.saga.replies| order
    inventory -->|order.saga.replies| order
    shipping -->|order.saga.replies| order
    order -.->|order.events| notification
    order --- odb[(order_db)]
    payment --- pdb[(payment_db)]
    inventory --- idb[(inventory_db)]
    shipping --- sdb[(shipping_db)]
    notification --- ndb[(notification_db)]
```

order-service is the **orchestrator**. It decides the next step and sends commands; payment, inventory and shipping only do what they're told and reply. When an order is approved or rejected, order-service also publishes an event on `order.events`; notification-service subscribes to it on its own (dotted line), and the orchestrator neither knows nor waits.

## Quick start (CMD)

Prerequisites: JDK 21, Maven 3.9+, Docker Desktop running. Details in [Getting started](docs/GETTING-STARTED.md).

```
docker compose up -d
mvn clean install -DskipITs
start "order"     java -jar order-service\target\order-service-0.0.1-SNAPSHOT.jar
start "payment"   java -jar payment-service\target\payment-service-0.0.1-SNAPSHOT.jar
start "inventory" java -jar inventory-service\target\inventory-service-0.0.1-SNAPSHOT.jar
start "shipping"  java -jar shipping-service\target\shipping-service-0.0.1-SNAPSHOT.jar
start "notify"    java -jar notification-service\target\notification-service-0.0.1-SNAPSHOT.jar
start "console"   java -jar saga-ui\target\saga-ui-0.0.1-SNAPSHOT.jar
start http://localhost:8080
```

The console asks you to sign in through Keycloak. Use `admin` / `admin` to run everything; `operator` and `viewer` have fewer rights. Admin endpoints need a token: `call ops\token.cmd admin admin`, then `curl -H "Authorization: Bearer %TOKEN%" …`. See [Getting started §4](docs/GETTING-STARTED.md#4-sign-in-users-roles-and-tokens).

In the console, click **Happy path**, then **Unshippable destination**, and compare the two routes on the state map: the second is charged and reserved, then both are undone in reverse order. To see the raw Kafka messages behind them, open **AKHQ** at http://localhost:8086; `docker compose up -d` starts it along with Postgres and Kafka.

## Documentation

Read in this order if you are new:

| Doc | What it gives you |
|---|---|
| [Getting started](docs/GETTING-STARTED.md) | Install, build, run, reset, test, inspect databases and Kafka, troubleshoot |
| [Architecture](docs/ARCHITECTURE.md) | Why a saga, how the pieces fit, every reliability mechanism and the failure modes it covers |
| [Scenarios](docs/SCENARIOS.md) | A hands-on lab of 17 scenarios, from the happy path to stuck compensations, each with steps, expected results and the code responsible |
| [Codebase tour](docs/CODEBASE.md) | Modules, packages, one order traced through the classes, where everything lives, how to extend it |
| [Runbook](ops/RUNBOOK.md) | Operator procedures for alerts, stuck sagas and dead letters |
| [Project log](PROJECT.md) | Status, decisions, configuration and endpoint reference, Boot 4 gotchas, open work |

## Repository layout

```
saga-common/        shared library: message contracts, outbox, idempotency, Kafka error handling, DLT replay
order-service/      REST API + saga orchestrator + timeouts + stuck-compensation alerting + interventions
payment-service/    charges and refunds customer credit
inventory-service/  reserves and releases stock
shipping-service/   schedules and cancels shipments; shipping zones decide which countries are served
notification-service/  tells customers the outcome, from order events (simulated e-mail gateway)
saga-ui/            browser console (proxy + Kafka message injector + single-page UI)
saga-e2e/           Testcontainers end-to-end tests that run the real jars
ops/                Prometheus alert rules (+ tests), operator runbook, token and image scripts
docker/, docker-compose.yml   local Postgres, Kafka, AKHQ (Kafka web UI on :8086) and Keycloak (:8180, realm in docker/keycloak/)
```

> **Security:** admin and seed endpoints and the console are protected with OAuth2/JWT (Keycloak). The identity setup is **development-grade**: Keycloak dev mode, dev users and secrets. See "Next / open" in [PROJECT.md](PROJECT.md).
