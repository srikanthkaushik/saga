# SAGA — Project status

> New here? Start with the [README](README.md) and the guides in [`docs/`](docs/): [Getting started](docs/GETTING-STARTED.md), [Architecture](docs/ARCHITECTURE.md), [Scenarios](docs/SCENARIOS.md), [Codebase tour](docs/CODEBASE.md). This file is the status and decision log.

Order saga using orchestration over Kafka, with choreographed domain events for what happens after the outcome. Java 21, Spring Boot 4.0.6, PostgreSQL 17, Kafka 4.0 (KRaft), multi-module Maven.

**Checkpoint 2026-10-16:** two former open items closed.
- **Per-service tests:** 220 tests in the `test` phase. The order saga state machine and orchestrator are tested without Spring, covering every legal and illegal transition and every reply in every wrong state. Each participant handler is a JPA slice against real Postgres, covering duplicate delivery, re-sent commands, tombstones and marker rollback.
- **Dead-letter alerting:** `saga_dlt_pending` on all five services, the rules `SagaDeadLettersPending` and `SagaDeadLetterMetricsMissing` with promtool tests, a Prometheus scrape job for every service, and runbook entries.
- 34 e2e tests green.

**Checkpoint 2026-10-10:** two more services. **shipping-service** is the saga's third step and its **pivot**: a refused shipment undoes stock and payment in reverse order; a shipping timeout cancels, releases and refunds (state CANCELLING_SHIPMENT). **notification-service** subscribes to `OrderApproved`/`OrderRejected` events on `order.events` (choreography) and is a retriable step: it never affects the order. 33 e2e tests green.

**Checkpoint 2026-10-09:** feature-complete for the core saga plus its failure handling: outbox, idempotency, DLT with backoff and replay, timeouts with fencing, stuck-compensation alerting, and manual resolution. A local **saga console** (`saga-ui`, http://localhost:8080) demos and exercises all of it. 23 e2e tests green, promtool rule tests green. Admin endpoints, seed endpoints and the console are secured with **OAuth2/JWT (Keycloak)**, with roles viewer, operator, admin and metrics. Not yet production-hardened: Keycloak runs in dev mode and the secrets are dev defaults (see [Next / open](#next--open)).

## Modules
| Module | Port | Role |
|---|---|---|
| `saga-common` | – | Message contracts, JSON codec, transactional outbox + relay, idempotent-consumer guard, Kafka error handling (retry/DLT), DLT replay and outbox actuator endpoints, topic beans |
| `order-service` | 8081 | REST API, saga orchestrator, timeout scanner, stuck-compensation metrics, `stucksagas` endpoint (list / detail / retry / resolve) |
| `payment-service` | 8082 | Debits and refunds customer credit; read/seed APIs for customers and payments |
| `inventory-service` | 8083 | Reserves and releases stock; read/seed APIs for products and reservations |
| `shipping-service` | 8084 | Schedules and cancels shipments (the pivot step); shipping zones decide which destinations are served; read/seed APIs for shipments and zones |
| `notification-service` | 8085 | Subscribes to order events; notifies the customer through a simulated e-mail provider that can be switched off; read API for notifications |
| `saga-ui` | 8080 (127.0.0.1) | Saga console: static single-page UI, same-origin proxy to the five services, Kafka message injector. No DB, no saga-common dependency |
| `saga-e2e` | random | Testcontainers end-to-end tests that run the real service and console jars as child processes |

Each service owns its database (`order_db`, `payment_db`, `inventory_db`, `shipping_db`, `notification_db`) on one Postgres instance. Other files:
- `docker-compose.yml`: Postgres, Kafka, **AKHQ** (Kafka web UI at http://localhost:8086) and **Keycloak** (http://localhost:8180, realm `saga` imported from `docker/keycloak/saga-realm.json`) for local runs.
  - Kafka advertises two client listeners: `localhost:9092` for host apps, and `kafka:29092` on the compose network (AKHQ).
  - Kafka data lives in the named volume `saga-kafka-data` (`KAFKA_LOG_DIRS=/var/lib/kafka/data`, a pinned `CLUSTER_ID`). Topics, messages and offsets survive re-creation; only `down -v` wipes them, together with Postgres.
- `docker\postgres\init.sql` (creates the five DBs; an existing volume needs `createdb` for the new ones, see Getting started §3.1)
- `ops\prometheus\` (alert rules and promtool tests)
- `ops\RUNBOOK.md` (operator procedures)

## Saga flow
```
POST /orders ─► Order(PENDING) + Saga(PAYMENT_PENDING) + outbox:ProcessPayment

PAYMENT_PENDING      PaymentProcessed  ─► INVENTORY_PENDING, outbox:ReserveInventory
                     PaymentFailed     ─► FAILED, order REJECTED
                     timeout           ─► COMPENSATING, outbox:RefundPayment            ("Payment timed out")
INVENTORY_PENDING    InventoryReserved ─► SHIPPING_PENDING, outbox:ScheduleShipment
                     InventoryFailed   ─► COMPENSATING, outbox:RefundPayment
                     timeout           ─► RELEASING_INVENTORY, outbox:ReleaseInventory  ("Inventory timed out")
SHIPPING_PENDING     ShipmentScheduled ─► COMPLETED, order APPROVED                     (the pivot)
                     ShipmentFailed    ─► RELEASING_INVENTORY, outbox:ReleaseInventory
                     timeout           ─► CANCELLING_SHIPMENT, outbox:CancelShipment    ("Shipping timed out")
CANCELLING_SHIPMENT  ShipmentCancelled ─► RELEASING_INVENTORY, outbox:ReleaseInventory
                     timeout           ─► same state, re-send CancelShipment, new deadline
RELEASING_INVENTORY  InventoryReleased ─► COMPENSATING, outbox:RefundPayment
                     timeout           ─► same state, re-send ReleaseInventory, new deadline
COMPENSATING         PaymentRefunded   ─► FAILED, order REJECTED (reason = original failure)
                     timeout           ─► same state, re-send RefundPayment, new deadline
any compensating state  operator retry   ─► same state, re-send now, stuck count reset
                        operator resolve ─► FAILED, order REJECTED "<reason> (compensation resolved manually)"

order APPROVED / REJECTED ─► outbox:OrderApproved / OrderRejected on order.events (same transaction) ─► notification-service
```
Replies that don't match the saga's current state (late, duplicate, out of order) are logged and dropped.

## Messaging
| Topic | Producer | Consumer | Messages |
|---|---|---|---|
| `payment.commands` | order | payment | ProcessPayment, RefundPayment |
| `inventory.commands` | order | inventory | ReserveInventory, ReleaseInventory |
| `shipping.commands` | order | shipping | ScheduleShipment, CancelShipment |
| `order.saga.replies` | payment, inventory, shipping | order | PaymentProcessed/Failed/Refunded, InventoryReserved/Failed/Released, ShipmentScheduled/Failed/Cancelled |
| `order.events` | order | notification | OrderApproved, OrderRejected (domain events; nobody replies) |
| `<topic>-dlt` (one per topic above) | Spring Kafka error handler | `dlt` replay endpoint | dead-lettered originals with `kafka_dlt-*` headers |

- All topics have 3 partitions (replication factor 1 locally). They are keyed by orderId, so every message for an order is ordered.
- DLT retention is 14 days.
- Records have a JSON value and two headers: `messageId` (the outbox row id, used as the idempotency key) and `messageType` (the record's simple name). Replayed records also carry `dltReplayCount`.

## Decisions
**Core**
- **Transactional outbox everywhere.** Business writes and outgoing messages commit in one DB transaction. `OutboxRelay` polls (500ms) with `FOR UPDATE SKIP LOCKED`. Delivery is at-least-once, and per-key order is preserved with a single relay. With several relay instances, ordering is best-effort.
- **Idempotent consumers.** `processed_message` uses `INSERT … ON CONFLICT DO NOTHING` in the handler's transaction. Participants also key on `order_id` (unique on `payment` and `reservation`).
- **Type registry.** It comes from the sealed `SagaCommand`/`SagaReply`/`OrderEvent` permits; a new message type is just a new record. The exhaustive `switch` in the orchestrator then fails to compile until the new reply is handled.
- **Package layout.** Main classes sit in package `com.saga`, so scanning covers `com.saga.common` without `@EntityScan` (which moved package in Boot 4). As a consequence, two services can't share a JVM, which is why e2e runs separate processes.
- **Pessimistic row locks** on `customer_credit`, `product`, `payment`, `reservation`, `shipment` and `shipping_zone` serialize concurrent work per row.

**Steps and events** (2026-10-10)
- **Compensatable → pivot → retriable.** Payment and inventory are compensatable; shipping is the pivot (last, its success completes the saga); notification is retriable and lives outside the saga.
- **Compensation runs in reverse.** The compensating states form one chain, CANCELLING_SHIPMENT → RELEASING_INVENTORY → COMPENSATING → FAILED; a failure enters it just below the last step that may have succeeded (ShipmentFailed skips cancelling, a shipping timeout doesn't).
- **Domain events through the outbox, in the outcome's transaction.** `OrderEventPublisher` is the only way to approve or reject an order, so the orchestrator paths and operator resolve all emit exactly one event.
- **Choreography for notification.** notification-service subscribes to `order.events`; order-service has no dependency on it. Sends are at-least-once (the provider is called before the row commits); `processed_message` and unique `(order_id, type)` make redeliveries and replays no-ops after a successful send.
- **Destination on the order** (`destination`, ISO country, optional, default GB) so existing clients keep working.
- **CANCELLING_SHIPMENT is a compensating state everywhere:** timeouts, stuck metrics, `stucksagas`, retry/resolve. Its SQL lists and partial index were updated in order V6.

**Failure handling**
- **Blocking retry with exponential backoff, then a DLT** (`KafkaErrorHandlingConfig`).
  - Default policy: 4 retries at 0.5s, 1s, 2s and 4s (about 7.5s), then `<topic>-dlt` on the same partition.
  - `NonRetryableMessageException` (malformed or unknown type, missing headers, reply for an unknown saga, wrong message on a topic) and `JacksonException` skip retries.
  - Blocking was chosen over `@RetryableTopic` to keep per-order ordering. The cost is that one bad record stalls its partition for the retry budget.
  - A `RetryListener` logs each failed attempt at WARN and each dead-letter at ERROR; Spring Kafka logs neither by default.
- **`spring.kafka.listener.concurrency: 3`** (one consumer thread per partition). With 1, a record in retry stalled the whole service.
- **DLT replay** (actuator `dlt`, in saga-common, so every service has it).
  - **Scope:** each service may replay only the DLTs of topics its own `@KafkaListener`s consume.
  - **Progress tracking:** committed offsets of the group `<app>-dlt-replay`, via manual `assign()`. "Pending" means not yet replayed.
  - **No loops:** replay works up to a snapshot of the end offsets, so a record that fails again waits for the next replay.
  - **Commit order:** offsets are committed only after all sends are acknowledged. A crash re-sends at most a batch; the unchanged `messageId` keeps that idempotent.
  - **Headers:** `kafka_dlt-*` headers are stripped and `dltReplayCount` goes up by one.
- **Saga timeouts** (`SagaTimeoutScanner`, `OrderSagaOrchestrator.onTimeout`).
  - Each non-terminal state carries `order_saga.deadline`. Each overdue saga is handled in its own transaction, and `@Version` serializes a reply against a timeout.
  - **A timeout compensates unconditionally**, because the participant may be slow rather than dead. The compensation shares the orderId key and partition, so it is always processed *after* the original command: a late charge is refunded and a late reservation is released.
  - **Compensation timeouts re-send forever.** Compensations are idempotent and must eventually succeed; alerting covers the case where they don't.
- **Tombstone fencing.**
  - A refund with no payment on record inserts a payment with status CANCELLED (customer and amount null).
  - A release with no reservation inserts a reservation with status RELEASED.
  - A cancel with no shipment inserts a shipment with status CANCELLED (placeholder product and destination).
  - A later ProcessPayment, ReserveInventory or ScheduleShipment for that order, for example replayed from a DLT, is refused with PaymentFailed, InventoryFailed or ShipmentFailed.
  - Refund, release and cancel always reply, so the saga never hangs on them.

**Operability**
- **Stuck-compensation alerting** (`CompensationMonitor`).
  - `compensation_resends` counts re-sends of the current compensation step and resets on entering a new one. Stuck means resends ≥ `saga.alert.stuck-after-resends`.
  - At the threshold there is one ERROR `STUCK COMPENSATION` log line, then an ERROR per further re-send. It is logged after commit only.
  - Gauges are refreshed from the DB on a schedule (not per scrape) and are database-wide, so the alert rules use `max()`, not `sum()`.
- **Dead-letter alerting** (`DltMetrics`, 2026-10-16).
  - `saga_dlt_pending{topic,dlt}` per owned DLT on every service: the same "not yet replayed" count as `GET /actuator/dlt`, so it drops to 0 after a replay even if a record fails again; it then comes back as new pending.
  - Refreshed on its **own daemon thread**, not `@Scheduled`: Boot's scheduler has one thread, and with Kafka down a refresh blocks for the consumer API timeout, which would delay the outbox relay and the timeout scanner. The DLT endpoint also records every read and replay, so the alert clears straight after an operator replays.
  - Gauges are registered on the first successful refresh; a service that never reached Kafka has no series, and `SagaDeadLetterMetricsMissing` covers that.
  - Alert severity is **ticket**: a dead-lettered compensation already pages via SagaCompensationStuck, and dead-lettered forward steps are compensated by timeouts.
- **Manual resolution** (`SagaInterventionService` via `stucksagas` write operation).
  - `retry` re-sends now and resets the stuck count.
  - `resolve` closes the saga as FAILED and requires a note. **The manual compensation must be done in the participant's own records first**; then any queued or replayed compensation is a no-op there. See `ops\RUNBOOK.md`.
  - Every intervention is recorded in `saga_intervention` (action, operator, note, from-state, to-state, time) and logged at WARN.
  - The operator is the **authenticated user** (the token's `preferred_username`, via the actuator `SecurityContext`). Any `operator` in the body is ignored.
- **Admin APIs are actuator endpoints**, not controllers, so they are off unless exposed.
- **Retention cleanup** (`RetentionJob`, 2026-10-16).
  - Deletes published outbox rows after 7 days and idempotency markers after 30 days, in batches of 1000 (one transaction each), picked with `SKIP LOCKED` so instances and the relay never block each other.
  - Unpublished rows are never deleted.
  - Markers outlive the 14-day DLT retention on purpose: a replay keeps the original `messageId`, and the marker is what makes it a no-op. Past that window the handlers' business-key checks (tombstones, state guard, unique notification) still hold.
  - Runs on the shared `@Scheduled` thread, capped per run (`max-batches-per-run`) so it never holds the outbox relay up for long. Counter `saga_retention_deleted_total{table}`.

**Security** (details in [Architecture §9](docs/ARCHITECTURE.md#9-security))
- **OAuth2/JWT via Keycloak.** The five services are resource servers sharing one rulebook, `SagaSecurityConfig` in saga-common: stateless, deny by default. They validate the signature (JWKS), the issuer `http://localhost:8180/realms/saga` and the audience `saga-api`. Roles are mapped from the `roles` claim by Boot properties (`authorities-claim-name`, `authority-prefix`, `principal-claim-name`); no custom converter.
- **Roles** are composite realm roles: `saga-viewer` ⊂ `saga-operator` ⊂ `saga-admin`, plus `saga-metrics`. The `roles` claim and the `saga-api` audience come from protocol mappers on each client, not from a realm-level client scope (a custom `clientScopes` list in an import would replace the built-in `profile`/`basic` scopes).
- **Public:** health, `POST /orders`, `GET /orders/{id}`. The customer-facing order API is out of scope for now.
- **Console:** an OIDC client (`saga-console`, auth code), with tokens in the server-side session, token relay through the proxy (`OAuth2AuthorizedClientManager` refreshes them), `csrf.spa()`, and RP-initiated logout. The page reads roles from `/api/me` and disables what the user can't do; the services enforce it regardless.
- **Other clients:** `saga-cli` (public, password grant, dev only; used by `ops\token.cmd` and e2e) and `saga-prometheus` (client credentials, role `saga-metrics`; see `ops/prometheus/prometheus.yml`).
- **Dev users:** `viewer`, `operator`, `admin` (password = username). Dev secrets in the realm file and in `saga-ui`'s `application.yml` (`SAGA_CONSOLE_CLIENT_SECRET` overrides).

## Configuration (`application.yml`, defaults shown)
| Property | Default | Where | Notes |
|---|---|---|---|
| `saga.outbox.poll-interval` | 500ms | all | relay poll |
| `saga.outbox.batch-size` | 100 | all | rows per relay tick |
| `saga.kafka.retry.max-retries` | 4 | all | then DLT |
| `saga.kafka.retry.initial-interval` | 500ms | all | |
| `saga.kafka.retry.multiplier` | 2.0 | all | |
| `saga.kafka.retry.max-interval` | 10s | all | |
| `spring.kafka.listener.concurrency` | 3 | all | keep equal to the partition count |
| `saga.timeout.payment` / `.inventory` / `.shipping` / `.compensation` | 30s | order | keep well above the retry budget (about 7.5s) |
| `saga.timeout.scan-interval` | 5s | order | |
| `saga.timeout.batch-size` | 100 | order | overdue sagas per scan |
| `saga.alert.stuck-after-resends` | 3 | order | about 2 minutes with 30s compensation timeouts |
| `saga.alert.refresh-interval` | 15s | order | gauge refresh |
| `saga.dlt.metrics-refresh-interval` | 30s | all | `saga_dlt_pending` refresh; opens a short-lived consumer per owned DLT |
| `saga.retention.enabled` | true | all | retention cleanup on or off |
| `saga.retention.interval` / `.initial-delay` | 1h / 1m | all | |
| `saga.retention.outbox-retention` | 7d | all | published outbox rows; the console timeline is lost with them |
| `saga.retention.processed-message-retention` | 30d | all | keep above the DLT retention (14d); a WARN at startup if not |
| `saga.retention.batch-size` / `.max-batches-per-run` | 1000 / 20 | all | one transaction per batch; at most 20000 rows per table per run |

## Saga console (`saga-ui`)
- **Saga tab:**
  - Scenario buttons seed a fresh customer and product, then place orders: happy path, insufficient credit, out of stock (refund), unshippable destination (release + refund), and a burst of ten. The order form has a destination.
  - Order detail has a **transit-map state machine** (4×2: forward steps along the top, compensations right to left along the bottom, no crossing tracks) lighting the route taken (cobalt = forward, amber = compensation), plus a **message timeline**: five swimlanes from the four outboxes and the notifications, with order events dashed and operator interventions included.
  - Payment, inventory, shipping and notification state cards.
  - The saga card shows the deadline countdown and re-sends, plus Retry/Resolve forms while compensating.
- **Operations tab:** compensation metric tiles, the stuck-saga list, the notification-provider switch, DLT pending counts with replay, the message injector (presets for non-retryable, retryable-crash, unknown-saga and malformed messages), and a timeout/stuck demo guide.
- **Reference data tab:** view and set credit, stock and shipping zones (open or close a destination).
- **API console tab:** a clickable catalogue of **every endpoint** across the five services and the console, with method, path, body and pretty-printed response.
- **Plumbing:**
  - `/api/{order|payment|inventory|shipping|notification}/**` is a pass-through proxy (status and body untouched; 502 JSON when a service is down; targets in `saga.ui.services.*`). `/api/inject` publishes raw records to the saga topics only.
  - Links are shareable: `#tab=ops`, `#order=<id>`.
  - Vanilla JS, no build step; server data is rendered with `textContent`. Light and dark themes.

## REST APIs
| Endpoint | Service | Purpose |
|---|---|---|
| `POST /orders` `{customerId, productId, quantity, amount, destination?}`, `GET /orders/{id}` | order | place an order (destination defaults to GB); order + saga state. **public** |
| `GET /orders?limit=50` | order | recent orders, newest first (limit 1–200). viewer |
| `GET /customers`, `GET /customers/{id}` | payment | credit. viewer |
| `PUT /customers/{id}` `{"availableCredit"}` | payment | set credit (seed tooling). **admin** |
| `GET /payments/{orderId}` | payment | COMPLETED / REFUNDED / CANCELLED tombstone. viewer |
| `GET /products`, `GET /products/{id}` | inventory | stock. viewer |
| `PUT /products/{id}` `{"availableQuantity"}` | inventory | set stock (seed tooling). **admin** |
| `GET /reservations/{orderId}` | inventory | RESERVED / RELEASED. viewer |
| `GET /shipments/{orderId}` | shipping | SCHEDULED (with tracking number) / CANCELLED. viewer |
| `GET /zones`, `GET /zones/{country}` | shipping | destinations and whether a carrier serves them. viewer |
| `PUT /zones/{country}` `{"served", "name"?}` | shipping | open, close or create a zone (seed tooling). **admin** |
| `GET /notifications?orderId=` (or `?limit=`) | notification | notifications sent. viewer |

Roles: viewer < operator < admin (composite); `saga-metrics` only reaches `/actuator/prometheus`. No token → 401, wrong role → 403, unlisted endpoint → 403.

## Actuator endpoints
| Endpoint | Service | Purpose |
|---|---|---|
| `GET /actuator/health` | all | liveness. **public** |
| `GET /actuator/info` | all | info. viewer |
| `GET /actuator/outbox/{orderId}` | all | messages this service emitted for an order (payload as JSON, created/published times). Merged across services = the saga timeline. viewer |
| `GET /actuator/dlt`, `GET /actuator/dlt/{topic}` | all | owned DLTs with pending counts. viewer |
| `POST /actuator/dlt/{topic}` body `{"limit":n}` | all | replay pending records (default 100, max 10000). **operator** |
| `GET /actuator/stucksagas` | order | stuck sagas, oldest first. viewer |
| `GET /actuator/stucksagas/{sagaId}` | order | saga detail + intervention history (any saga). viewer |
| `POST /actuator/stucksagas/{sagaId}` body `{"action","note"}` | order | `retry` / `resolve`, recorded under the token's user. **operator**. 400 bad request, 404 unknown saga, 409 not compensating or concurrent change |
| `GET /actuator/notificationprovider` | notification | provider available, sent and refused counts. viewer |
| `POST /actuator/notificationprovider` body `{"available"}` | notification | switch the simulated provider off or on. **operator** |
| `GET /actuator/metrics` | order | Micrometer. viewer |
| `GET /actuator/prometheus` | all | Prometheus scrape. metrics or viewer |

Metrics:
- `saga_compensation_stuck`
- `saga_compensation_in_progress`
- `saga_compensation_oldest_age_seconds`
- `saga_compensation_resends_total`
- `saga_dlt_pending{topic,dlt}` (every service, one series per owned DLT; refreshed every `saga.dlt.metrics-refresh-interval`, default 30s, and after each DLT read or replay)

Alert rules (`ops\prometheus\saga-alerts.yml`):
- `SagaCompensationStuck` (page)
- `SagaCompensationSlow` (ticket, oldest over 10m)
- `SagaCompensationMetricsMissing` (ticket)
- `SagaDeadLettersPending` (ticket, per topic, `max by (topic)` over instances)
- `SagaDeadLetterMetricsMissing` (ticket, per topic)

Prometheus and Alertmanager are **not** part of docker-compose.

## Database migrations (Flyway)
| Service | Migration | Change |
|---|---|---|
| order | V1 | orders, order_saga, outbox, processed_message |
| order | V2 | `order_saga.deadline` (+ partial index); in-flight sagas get now + 1m |
| order | V3 | `compensating_since`, `compensation_resends` (+ partial index) |
| order | V4 | `saga_intervention` audit table |
| payment | V1 | customer_credit, payment, outbox, processed_message + seed |
| payment | V2 | payment customer_id/amount nullable for CANCELLED tombstones (+ check constraint) |
| inventory | V1 | product, reservation, outbox, processed_message + seed |
| inventory | V2 | `reservation.status`, `released_at` |
| order V5 / payment V3 / inventory V3 | – | `outbox(message_key)` index for the outbox endpoint |
| order | V6 | `orders.destination` (default GB); compensating partial index recreated with CANCELLING_SHIPMENT |
| shipping | V1 | shipping_zone (+ seed GB/US/DE served, XX not), shipment, outbox, processed_message |
| notification | V1 | notification (unique order_id + type), outbox (unused), processed_message |
| order V7 / payment V4 / inventory V4 / shipping V2 / notification V2 | – | `outbox(published_at)` and `processed_message(processed_at)` indexes for the retention cleanup |

## Container images
- **`ops\images.cmd <tag> [push]`** packages the jars and builds eight images, `${SAGA_REGISTRY}/saga-*` (default `ghcr.io/srikanthkaushik`):
  - `saga-order-service`, `saga-payment-service`, `saga-inventory-service`, `saga-shipping-service`, `saga-notification-service` and `saga-console`, from one layered, non-root `docker/app.Dockerfile` (an app update re-pushes about 340 kB; the 91 MB dependency layer is cached);
  - `saga-keycloak`, with the realm baked in;
  - `saga-postgres`, with the init script.
- **Configuration:** the apps are configured through environment variables (Spring relaxed binding), e.g. `SPRING_DATASOURCE_URL`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_ISSUER_URI`, `SAGA_UI_SERVICES_ORDER`. The console needs `SERVER_ADDRESS=0.0.0.0` in a container (it defaults to 127.0.0.1).
- **Realm placeholders** `${VAR:default}`: `SAGA_CONSOLE_URL`, `SAGA_CONSOLE_CLIENT_SECRET`, `SAGA_PROMETHEUS_CLIENT_SECRET` and `SAGA_{VIEWER,OPERATOR,ADMIN}_PASSWORD` can be set on the Keycloak container. Unset, they fall back to the dev values used locally and in e2e. This form is undocumented but verified on Keycloak 26.7.0, hence the pinned image.
- **One public Keycloak URL:** a deployment must give Keycloak a single public URL (`KC_HOSTNAME`), because it becomes the token issuer. Browsers and the services must reach Keycloak by exactly that URL.

## Boot 4 gotchas (verified)
- Starters: `spring-boot-starter-webmvc` (not `-web`), `spring-boot-starter-kafka`, and `spring-boot-starter-flyway` + `flyway-database-postgresql`. Prometheus export: `io.micrometer:micrometer-registry-prometheus`.
- Jackson 3: inject `tools.jackson.databind.json.JsonMapper`. Exceptions are unchecked, and `JsonNode.asText()` is now `asString()`.
- Kafka JSON serde classes are now `JacksonJsonSerializer`/`JacksonJsonDeserializer` (unused here; String serde + own codec).
- `@EntityScan` lives in `org.springframework.boot.persistence.autoconfigure` (avoided via package layout).
- **Test slices are separate modules.** `@DataJpaTest` comes from `spring-boot-starter-data-jpa-test` (package `org.springframework.boot.data.jpa.test.autoconfigure`); `@AutoConfigureTestDatabase` is in `org.springframework.boot.jdbc.test.autoconfigure`. Flyway still runs in the slice: `spring-boot-flyway` registers itself under `AutoConfigureDataSourceInitialization`. `@DataJpaTest` turns SQL logging on by default (`showSql = false` turns it off).
- `spring-boot-actuator` (the non-starter module that saga-common uses) does **not** bring `micrometer-core`; declare it.
- Spring Kafka's default DLT suffix is `-dlt` (not `.DLT`). A `CommonErrorHandler` bean is auto-wired into Boot's listener factory.
- Custom actuator endpoints: optional params use jspecify `@Nullable`; write-op params bind from the JSON body; `WebEndpointResponse` sets the status.
- **Don't put `@Validated` on `@RestController`s.** Spring MVC (6.1+) validates constraint-annotated params itself and answers 400 (`HandlerMethodValidationException`). Class-level `@Validated` routes them through the AOP `MethodValidationInterceptor` instead, whose `ConstraintViolationException` surfaces as a 500.
- Proxying with `RestClient`: use `exchange(...)` to pass upstream 4xx/5xx through untouched (no status handlers run), and `JdkClientHttpRequestFactory` for connect and read timeouts.
- Testcontainers 2.x: `testcontainers-postgresql`/`-kafka`/`-junit-jupiter`. Use `org.testcontainers.postgresql.PostgreSQLContainer` (non-generic) and `org.testcontainers.kafka.KafkaContainer`.
- A fresh broker logs `NotCoordinatorException` for a few seconds while `__consumer_offsets` is created. Harmless.
- **Security starters:** `spring-boot-starter-oauth2-resource-server` and `spring-boot-starter-oauth2-client` (unchanged names). Boot's JWT properties `audiences`, `authorities-claim-name`, `authority-prefix` and `principal-claim-name` replace a custom `JwtAuthenticationConverter`.
- **Spring Security 7 `csrf.spa()`** sets up the `XSRF-TOKEN` cookie plus `X-XSRF-TOKEN` header pattern in one line.
- **`oauth2Login` only redirects requests that accept `text/html`.** curl (`Accept: */*`) falls through to the other entry point, so the console answers it with a 401.
- **With a `GrantedAuthoritiesMapper`, the mapped roles are on the `Authentication`**, not on the `OidcUser` principal. `@AuthenticationPrincipal OidcUser#getAuthorities()` returns the unmapped ones.
- **The OAuth2 client resolves `issuer-uri` at startup,** so the console fails to start while Keycloak is down. Resource servers resolve it lazily, on the first token.
- **Keycloak 26:** bootstrap admin is `KC_BOOTSTRAP_ADMIN_USERNAME/PASSWORD`; `start-dev --import-realm` skips realms that already exist; users need first name, last name and email, or the login asks for them (and the password grant fails).
- **A Postgres healthcheck that other containers depend on must use TCP** (`pg_isready -h 127.0.0.1`). During first-start initialisation Postgres listens only on its Unix socket, so a socket check reports ready while other containers' connections are refused.
- **`crypto.randomUUID` exists only in secure contexts** (HTTPS or localhost). Over plain HTTP on any other address it is missing, so the console falls back to `crypto.getRandomValues`.
- **Keycloak over plain HTTP on a non-localhost address** sets its login cookies `SameSite=Lax` without `Secure`, so browser login still works. `KC_HOSTNAME=http://host:port` fixes the issuer regardless of the address used.
- **Keycloak login cookies are `Secure; SameSite=None`.** Browsers send them to `http://localhost`, but `java.net.CookieManager` does not. The e2e login client strips `Secure` (`ConsoleSession.LocalhostCookies`).

## Run (CMD)
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

rem protected endpoints: get a token first (dev users viewer/operator/admin, password = username)
call ops\token.cmd admin admin
curl -H "Authorization: Bearer %TOKEN%" http://localhost:8081/actuator/stucksagas

curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"customer-1\",\"productId\":\"product-1\",\"quantity\":2,\"amount\":100.00}"
curl http://localhost:8081/orders/<id>
```
- Seed data: `customer-1` has 1000.00 credit and `customer-2` 50.00. `product-1` has qty 100; `product-2` has qty 0, which forces a compensation. Shipping zones GB, US and DE are served; XX is not, which forces a release and a refund.
- Operator commands (DLT replay, stuck sagas, retry/resolve) are in `ops\RUNBOOK.md`; the console does the same from its Operations tab.
- **Kafka UI:** AKHQ at http://localhost:8086 (cluster `saga-local`; started by `docker compose up -d`). It's read-write and unauthenticated (local only). Replay DLTs with the `dlt` endpoint, not AKHQ's produce or copy.
- **Editing the console:** to work on the UI without rebuilding, start it with `--spring.web.resources.static-locations=file:saga-ui/src/main/resources/static/ --spring.web.resources.cache.period=0` and refresh the browser.
- **Faster timeout demo:** start order-service with `--saga.timeout.payment=10s --saga.timeout.inventory=10s --saga.timeout.shipping=10s --saga.timeout.compensation=10s`.

## Tests
- `mvn test`: per-service tests, about a minute (see [Codebase tour §5.1](docs/CODEBASE.md#51-per-service-tests)):
  - `OrderSagaTest` (108) and `OrderSagaOrchestratorTest` (89): plain JUnit and Mockito, no Spring;
  - `PaymentCommandHandlerTest` (6), `RetentionJobTest` (4), `InventoryCommandHandlerTest` (7), `ShippingCommandHandlerTest` (6) and `NotificationHandlerTest` (4): `@PostgresSliceTest` (`@DataJpaTest` + Testcontainers Postgres 17 + the service's Flyway migrations), from the saga-common **test-jar**.
- `mvn clean verify` also runs `SagaEndToEndIT`: 34 tests, about 3 minutes. Docker is required; both suites are skipped automatically without it. Add `-DskipITs` to skip the e2e suite only.
- Slice-test conventions:
  - each test uses fresh UUID ids and runs in a rolled-back transaction;
  - tests that need the handler's own commit or rollback are `@Transactional(propagation = NOT_SUPPORTED)`;
  - replies are read back from the outbox table, decoded by the real codec.
  - `-Dmaven.test.skip=true` breaks the build: it skips the test-jar the services depend on. Use `-DskipTests`.
- Alert rules are tested separately:
  ```
  docker run --rm -v "%cd%\ops\prometheus":/rules --entrypoint promtool prom/prometheus test rules /rules/saga-alerts.test.yml
  ```

**Harness**
- Testcontainers starts Postgres 17 (with `docker\postgres\init.sql`), `apache/kafka:4.0.0` and Keycloak 26.7 (with the same `docker\keycloak\saga-realm.json`).
- Protected calls use an `admin` token by default, interventions an `operator` token. Placing and reading orders stays token-less, which proves those endpoints are public.
- The packaged jars are launched as child JVMs on free ports, with test settings: 1s/2s/4s backoff, 3s step timeouts, a 250ms scan, and a stuck threshold of 2.
- Service logs go to `saga-e2e\target\e2e-logs\`. `KafkaProbe` injects raw records and reads DLTs.
- Each test seeds its own customer and product (UUID ids) over JDBC.
- Tests that poison partitions or install DB triggers run last (`@Order`), so the replay tests don't replay their poison records.

| Area | What's proven |
|---|---|
| Happy / business paths | approve (shipment SCHEDULED with tracking number, OrderApproved notified); default destination GB; insufficient credit; unknown customer; out of stock → refund; 10 concurrent orders on 50 stock → exactly 5 approved and 5 refunded; 400 on invalid input |
| DLT | non-retryable → DLT in under 7s with headers kept; unknown-saga reply → DLT and the flow keeps working; retryable NPE → DLT only after the full backoff (at least 7s), with no side effects |
| DLT metrics | every service exports `saga_dlt_pending` for exactly the DLT it owns; a dead-lettered reply raises order-service's gauge |
| Replay | a reply that arrived before its saga, replayed after the cause is fixed → APPROVED; a still-broken record returns with `dltReplayCount=1`; endpoint scoping and 400s |
| Timeouts | a stalled participant (poison on every partition) → "Payment timed out" / "Inventory timed out"; the late charge is refunded and the late reservation released |
| Shipping | unshippable destination → release then refund, in that order; closing and reopening a zone; shipping timeout (poison stall) → late shipment cancelled, stock released, payment refunded |
| Notifications | OrderApproved / OrderRejected events in the order outbox; a notification per outcome, including after operator resolve; provider down → order still approved, event dead-lettered; provider back + replay → exactly one notification, also after a second replay |
| Fencing | refund-before-charge, release-before-reserve and cancel-before-schedule leave tombstones, and the late command is refused |
| Alerting | a failing refund (DB trigger) → appears in `stucksagas`, gauge ≥ 1, Prometheus output present; clears after the fix |
| Intervention | retry resets the count and is audited; resolve after a manual refund → FAILED/REJECTED and the late queued refund is a no-op (no double refund); 400/404/409 cases |
| Read / seed APIs | payment and reservation status, credit and stock reads; outboxes give exactly `ProcessPayment, ReserveInventory, RefundPayment, OrderRejected` / `PaymentProcessed, PaymentRefunded` / `InventoryFailed` for a compensated order; orders list newest-first; PUT upsert, and 400 for negative values or a bad id |
| Console (`saga-ui`) | serves the page; seed and order **through the proxy** → APPROVED; upstream 404 passes through; unknown service 404; Prometheus via proxy; `/api/inject` lands on the DLT; non-saga topic → 400 |
| Security | role matrix (none/viewer/operator/admin × health, actuator read, order list, credit read, DLT replay, seed PUT, unlisted, zone read, zone PUT, notification read, provider switch) = 401/403/200 as designed; wrong-realm and garbage tokens → 401 with `WWW-Authenticate: Bearer`; `saga-prometheus` scrapes but can't read DLTs; audit records the token's user even when the body says otherwise; console: redirect to Keycloak, 401 with a marker header for `/api`, scripted login, token relay, CSRF required, viewer can't replay or inject, operator can't seed |
| Alert rules (promtool) | stuck fires once with two instances (`max`), ignores blips shorter than `for`; slow and missing alerts fire. Checked to fail if `max` is swapped for `sum` |

## Done
- [x] 2026-10-08: scaffold, outbox, idempotency, orchestration (manually verified end to end)
- [x] Testcontainers e2e harness
- [x] Dead-letter topics + exponential backoff + non-retryable classification
- [x] DLT replay actuator endpoint
- [x] Saga timeouts + ReleaseInventory compensation + tombstone fencing + listener concurrency 3
- [x] Stuck-compensation alerting: metrics, Prometheus export, alert rules + promtool tests
- [x] Manual resolution: retry/resolve with audit trail, saga detail view, operator runbook
- [x] 2026-10-09: **Container images**: 6 images and `ops\images.cmd`; realm placeholders for URL, secrets and passwords; the console works over plain-HTTP origins.
- [x] 2026-10-09: **Security**: Keycloak 26.7 + OAuth2/JWT resource servers + console OIDC login with token relay and CSRF + role-aware UI + `ops\token.cmd` + Prometheus client credentials. 27 e2e tests green, plus a headless-Chrome login check per role.
- [x] 2026-10-09: Kafka named volume + pinned CLUSTER_ID. Verified that topics, message counts and committed offsets are identical after `--force-recreate`, and orders keep flowing.
- [x] 2026-10-09: AKHQ 0.28.0 Kafka UI in docker-compose (port 8086) plus a `DOCKER` listener on Kafka for container clients. Verified against Kafka 4.0: topics, counts, lag, DLT records with headers, and groups (3 members each).
- [x] 2026-10-09: saga console (`saga-ui`) + read/seed APIs + outbox endpoint.
  - Checked in headless Chrome over CDP: every scenario renders the right status, map route and message count; ops tiles, DLT replay, inject, data save, API console, deep links and intervene (resolve without a note is refused; retry completes the saga) all work, with no JS errors.
  - That browser check is a one-off script and is **not** in the repo.

- [x] 2026-10-10: **shipping-service** (pivot, reverse-order compensation, CANCELLING_SHIPMENT, zones, fencing) and **notification-service** (order events on `order.events`, choreography, retriable step with provider outage, exactly-once notification). Console: 4×2 map, five lanes, destination, zones, provider switch, Unshippable destination scenario. 33 e2e tests green; headless-Chrome check of every new view; manual outage check (shipping-service stopped mid-saga → CANCELLING_SHIPMENT → restart → late shipment cancelled, stock and credit restored, customer notified).
- [x] 2026-10-12: Architecture guide explains orchestration vs choreography: what each is, a comparison, where each is used here (saga commands and replies vs `order.events`), why, the outbox hand-over between them, how to see it in the console, and which style a new service should use.
- [x] 2026-10-12: recorded the RESTEasy/JAX-RS port analysis (see "Evaluated: porting off Spring Boot").
- [x] 2026-10-16: **Per-service tests**: 220 tests (state machine and orchestrator without Spring; participant handlers as Postgres slices). Shared `@PostgresSliceTest` and `TestMessages` in the saga-common test-jar.
- [x] 2026-10-16: **Retention cleanup**: `RetentionJob` (published outbox 7d, markers 30d, batched, `SKIP LOCKED`), index migrations on all five services, `RetentionJobTest` (4) against Postgres.
- [x] 2026-10-16: **Dead-letter alerting**:
  - `DltMetrics` (`saga_dlt_pending{topic,dlt}`, own refresh thread, updated on replay);
  - Prometheus export on all five services;
  - `SagaDeadLettersPending` and `SagaDeadLetterMetricsMissing` with promtool tests;
  - a single `saga-services` scrape job;
  - runbook entries.
  34 e2e tests green.

## Evaluated: porting off Spring Boot (RESTEasy / JAX-RS), 2026-10-12

**Question:** could the whole application use RESTEasy (JAX-RS) instead of Spring Boot?

**Verdict:** possible, but a full rewrite with no functional gain. RESTEasy replaces only the REST layer. Most of what Spring Boot does here isn't REST, and the real cost is in the Kafka retry/dead-letter handling and security. **Spring Boot 4.0.6 stays the canonical stack** (CLAUDE.md).

- **Namespace:** `javax.ws.rs` is end-of-life (RESTEasy 3.x/4.x, before Jakarta EE 9). A Java 21 port would use `jakarta.ws.rs` (RESTEasy 6.2/7.x).
- **Measured coupling:** 57 of the 89 main classes import Spring. The heaviest areas are web binding, Spring Data JPA, `@Transactional`, actuator and security.

| Spring feature used here | Replacement | Effort |
|---|---|---|
| `@RestController`, request mapping, validation | JAX-RS resources + Bean Validation | Mechanical |
| Spring Data JPA repositories, entities | Hibernate / Jakarta Persistence directly; repositories by hand (or Jakarta Data / Panache) | Moderate |
| `@Transactional`, incl. `MANDATORY` (outbox written in the caller's transaction, `OrderEventPublisher`) | Jakarta Transactions `@Transactional(TxType.MANDATORY)`; needs a CDI container | Easy in a container, hard without |
| Spring Kafka: `@KafkaListener`, blocking retry with backoff, dead-letter publishing, `KafkaListenerEndpointRegistry` (the pause/replay behind `DltEndpoint`) | SmallRye Reactive Messaging (dead-letter-queue strategy) or the raw Kafka client with a hand-written retry/DLT loop | **Largest.** All DLT/retry/replay behaviour hangs on it |
| Spring Security: JWT resource server, role rules (`SagaSecurityConfig`), the console's OAuth2 login with session and CSRF | MicroProfile JWT or an OIDC extension; without one, a `ContainerRequestFilter` + JWT library + a hand-written login flow | Large; easy to get subtly wrong |
| Actuator: health, Prometheus, custom endpoints (`dlt`, `stucksagas`, `notificationprovider`) | MicroProfile Health + Micrometer; the custom endpoints become ordinary JAX-RS resources | Moderate |
| `@Scheduled` (outbox relay, `SagaTimeoutScanner`) | Jakarta EE `@Schedule` / a managed scheduled executor | Easy |
| `@ConfigurationProperties`, profiles | MicroProfile Config | Easy |
| e2e harness (starts the Boot jars) | Change how the services are launched | Moderate |

**Carries over unchanged:**
- `OrderSaga` and its transitions, the message records, `MessageCodec`;
- the Flyway migrations, the topic design, the Keycloak realm, the console's static files;
- the design itself: outbox, idempotency, fencing, timeouts.

**Options, ranked:**
1. **Quarkus.** RESTEasy-based REST, SmallRye Kafka with dead-letter support, Hibernate, OIDC, Health, Micrometer. The closest match and the least hand-wiring.
2. **A Jakarta EE / MicroProfile server** such as WildFly, which ships RESTEasy. Weaker Kafka support.
3. **Plain embedded RESTEasy** (Undertow/Netty), with every library assembled by hand. Rewrites the transactions, retry/DLT and security; the most work and the most risk.

**If ever tried:** pilot one participant service (inventory or shipping) on Quarkus, and run it against the unchanged e2e suite. It tests over HTTP and Kafka, not Spring internals, so it checks the port as it stands.

## Next / open
- [ ] **Production identity setup:**
  - Keycloak in production mode (`start`, a real database, HTTPS, its own hostname);
  - secrets from a vault or the environment instead of the realm file;
  - no dev users, no `saga-cli` password grant;
  - shorter-lived console sessions.
- [ ] Secure the customer-facing order API (`POST /orders`, `GET /orders/{id}`). Customers would need their own role or client, and to be limited to their own orders.
- [ ] AKHQ is still unauthenticated (dev only); it supports OIDC if it ever leaves local development.
- [ ] Decide whether `saga-ui` should exist outside local/dev at all (it binds 127.0.0.1 by default).
- [ ] Browser tests for the console in CI (e.g. Playwright for Java), if it becomes more than a dev tool.
- [ ] Participant-side admin operations (refund or release through payment and inventory endpoints) so `resolve` never needs raw SQL.
- [ ] Selective DLT replay (by messageId or offset range) and a "skip" operation for known-garbage records.
- [ ] Alert on outbox lag (oldest unpublished row): the cleanup never deletes unpublished rows, so a stuck relay still grows the table.
- [ ] CI: run `mvn verify` and the promtool rule tests.
- [ ] Externalize DB/Kafka credentials. Topic replication factor 1 is local-only.
- [ ] Consider Debezium CDC instead of the polling relay if latency or DB load matters.
