# Getting started

Everything here is for **Windows Command Prompt (CMD)**. All commands run from the repository root (`C:\DEVL\SAGA` or wherever you cloned it) unless stated otherwise.

- [1. Prerequisites](#1-prerequisites)
- [2. Build](#2-build)
- [3. Start everything](#3-start-everything)
- [4. Your first order](#4-your-first-order)
- [5. Configuration overrides](#5-configuration-overrides)
- [6. Stop and reset](#6-stop-and-reset)
- [7. Run the tests](#7-run-the-tests)
- [8. Look inside: databases and Kafka](#8-look-inside-databases-and-kafka)
- [9. Troubleshooting](#9-troubleshooting)

## 1. Prerequisites

| Tool | Version | Check |
|---|---|---|
| JDK | 21 | `java -version` |
| Maven | 3.9+ | `mvn -v` (must report Java 21) |
| Docker Desktop | any recent, **running** | `docker version` (both Client and Server sections) |
| curl | ships with Windows 10+ | `curl --version` |
| A browser | – | for the console at http://localhost:8080 |

Ports that must be free: **5432** (Postgres), **9092** (Kafka), **8080–8083** (console and services). Check one with `netstat -ano | findstr :8081`.

## 2. Build

```
mvn clean install -DskipITs
```

This builds all six modules in about 10 s, skipping the end-to-end suite (which needs Docker and takes about 2 minutes, see [section 7](#7-run-the-tests)). The runnable jars land in:

```
order-service\target\order-service-0.0.1-SNAPSHOT.jar
payment-service\target\payment-service-0.0.1-SNAPSHOT.jar
inventory-service\target\inventory-service-0.0.1-SNAPSHOT.jar
saga-ui\target\saga-ui-0.0.1-SNAPSHOT.jar
```

`saga-common` is a library jar inside each service; it isn't run on its own.

## 3. Start everything

### 3.1 Infrastructure
```
docker compose up -d
docker compose ps
```
This starts `saga-postgres` (Postgres 17, user `saga` / password `saga`) and `saga-kafka` (Kafka 4.0 in KRaft mode, a single broker). On first start, `docker\postgres\init.sql` creates the three databases `order_db`, `payment_db` and `inventory_db`. Data persists in the Docker volume `saga_saga-postgres-data`.

### 3.2 Services
Each command opens its own window, so you can watch the logs:
```
start "order"     java -jar order-service\target\order-service-0.0.1-SNAPSHOT.jar
start "payment"   java -jar payment-service\target\payment-service-0.0.1-SNAPSHOT.jar
start "inventory" java -jar inventory-service\target\inventory-service-0.0.1-SNAPSHOT.jar
```
On startup each service:
- runs its Flyway migrations (creating tables and seed data in its own database);
- creates the Kafka topics it needs (if missing);
- starts 3 consumer threads;
- starts its outbox relay.

Each is ready after about 10 s:

```
curl http://localhost:8081/actuator/health
curl http://localhost:8082/actuator/health
curl http://localhost:8083/actuator/health
```
Each should answer `{"groups":["liveness","readiness"],"status":"UP"}`.

> On a brand-new Kafka you may see `NotCoordinatorException` lines for a few seconds while Kafka creates its internal `__consumer_offsets` topic. They're harmless.

### 3.3 Console (optional but recommended)
```
start "console" java -jar saga-ui\target\saga-ui-0.0.1-SNAPSHOT.jar
start http://localhost:8080
```
The three dots at the top of the console show service health (green = up).

### 3.4 Seed data
The Flyway migrations create these:

| Id | Value | Purpose |
|---|---|---|
| `customer-1` | 1000.00 credit | general use |
| `customer-2` | 50.00 credit | too little for most orders |
| `product-1` | 100 in stock | general use |
| `product-2` | 0 in stock | always fails inventory, so it triggers a refund |

You can create or overwrite any customer or product at any time:
```
curl -X PUT http://localhost:8082/customers/alice -H "Content-Type: application/json" -d "{\"availableCredit\":500}"
curl -X PUT http://localhost:8083/products/widget -H "Content-Type: application/json" -d "{\"availableQuantity\":10}"
```

## 4. Your first order

```
curl -i -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"customer-1\",\"productId\":\"product-1\",\"quantity\":2,\"amount\":100.00}"
```
The response is `201 Created`, with `"status":"PENDING"` and `"sagaState":"PAYMENT_PENDING"`. The saga runs asynchronously. About a second later:
```
curl http://localhost:8081/orders/<id from the response>
```
shows `"status":"APPROVED"` and `"sagaState":"COMPLETED"`.

Now go to [Scenarios](SCENARIOS.md) and work through them in order.

## 5. Configuration overrides

Any property can be overridden on the command line with `--name=value` after the jar name. The defaults are listed in [PROJECT.md › Configuration](../PROJECT.md#configuration-applicationyml-defaults-shown). The most useful ones for exploring:

| Goal | Start order-service with |
|---|---|
| See timeouts within seconds instead of 30 s | `--saga.timeout.payment=10s --saga.timeout.inventory=10s --saga.timeout.compensation=10s` |
| See the stuck gauge update quickly | `--saga.alert.refresh-interval=2s` |
| Mark a saga stuck sooner | `--saga.alert.stuck-after-resends=2` |

For example:
```
start "order" java -jar order-service\target\order-service-0.0.1-SNAPSHOT.jar --saga.timeout.payment=10s --saga.timeout.inventory=10s --saga.timeout.compensation=10s --saga.alert.refresh-interval=2s
```
[Scenarios 8–12](SCENARIOS.md) assume these fast settings.

The backoff for every service is set with `--saga.kafka.retry.max-retries=4 --saga.kafka.retry.initial-interval=500ms`. The console's service URLs are set with `--saga.ui.services.order=http://localhost:8081` (and likewise for `payment` and `inventory`).

## 6. Stop and reset

| Goal | Do |
|---|---|
| Stop a service | Close its window, or press Ctrl+C in it |
| Stop infrastructure, keep data | `docker compose down` |
| **Wipe everything** (databases and Kafka topics) | `docker compose down -v`, then `docker compose up -d` and restart the services so Flyway recreates the schema |

## 7. Run the tests

### End-to-end suite
```
mvn clean verify
```
This builds everything, then `saga-e2e` runs `SagaEndToEndIT`: **23 tests in about 2 minutes**. It uses Testcontainers to start its *own* Postgres and Kafka on random ports, so it doesn't touch your `docker compose` data. It then launches the four real jars as child processes.
- **Docker must be running.** Without Docker the suite is skipped, not failed.
- **Stop your locally running services first.** `clean` can't delete a jar that a running service has open on Windows.
- **Where to look:**
  - Service logs from the test run: `saga-e2e\target\e2e-logs\` (one file per service)
  - Test reports: `saga-e2e\target\failsafe-reports\`

Run a single test:
```
mvn verify -pl saga-e2e -am "-Dit.test=SagaEndToEndIT#happyPath_approvesOrder_debitsCreditAndReservesStock"
```

The tests are mapped to scenarios in [SCENARIOS.md › Scenario 14](SCENARIOS.md#scenario-14--the-automated-suite).

### Alert-rule tests
The Prometheus rules ship with promtool unit tests:
```
docker run --rm -v "%cd%\ops\prometheus":/rules --entrypoint promtool prom/prometheus test rules /rules/saga-alerts.test.yml
```
This should print `SUCCESS`.

## 8. Look inside: databases and Kafka

### Postgres
Open a SQL shell on one service's database:
```
docker exec -it saga-postgres psql -U saga -d order_db
```
(`\dt` lists tables, `\q` quits.) Or run one query directly:
```
docker exec saga-postgres psql -U saga -d order_db -c "select id, status, rejection_reason from orders order by created_at desc limit 5;"
docker exec saga-postgres psql -U saga -d order_db -c "select order_id, state, deadline, compensation_resends from order_saga order by created_at desc limit 5;"
docker exec saga-postgres psql -U saga -d payment_db -c "select * from customer_credit;"
docker exec saga-postgres psql -U saga -d payment_db -c "select order_id, status, amount from payment order by created_at desc limit 5;"
docker exec saga-postgres psql -U saga -d inventory_db -c "select * from product;"
docker exec saga-postgres psql -U saga -d order_db -c "select message_type, published_at is not null as sent, created_at from outbox order by created_at desc limit 10;"
```

### Kafka
The Kafka command-line tools live inside the container, at `/opt/kafka/bin`:
```
docker exec saga-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec saga-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payment-service
docker exec saga-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order.saga.replies --from-beginning --property print.key=true --property print.headers=true --timeout-ms 5000
```
- **Topics:** `payment.commands`, `inventory.commands` and `order.saga.replies`, each with a `-dlt` twin, plus Kafka's internal `__consumer_offsets`.
- **Consumer groups:** `order-service`, `payment-service` and `inventory-service`. `<service>-dlt-replay` groups appear after the first DLT replay.
- **Lag:** `--describe` shows the lag per partition. A growing lag means the service is down or stuck in retries.

### HTTP
Every endpoint is listed, with examples, in the console's **API console** tab. They're also tabulated in [PROJECT.md](../PROJECT.md#rest-apis).

## 9. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Port 8081 was already in use` | Another instance is still running. Find it with `netstat -ano \| findstr :8081`, then stop it (`taskkill /PID <pid> /F`) |
| Service fails with `Connection to localhost:5432 refused` | Postgres isn't up yet: `docker compose ps`, wait for `healthy` |
| Service logs `Node -1 disconnected` / `Connection to node … could not be established` | Kafka isn't up: `docker compose up -d` |
| `NotCoordinatorException` for a few seconds after a fresh start | Kafka is creating `__consumer_offsets`. Harmless |
| `Unable to access jarfile …` | Not built yet: `mvn clean install -DskipITs` |
| `mvn clean …` fails to delete `target\…jar` | A local service is running from that jar; stop it first |
| Flyway `Validate failed: Migration checksum mismatch` | An already-applied migration file was edited. Never edit applied migrations; add a new `V<n>__…sql`. Locally you can reset with `docker compose down -v` |
| An order stays `PENDING` | A participant is down or a message is stuck. Check health, then `GET /actuator/dlt` on each service and the service windows for `Delivery attempt … failed`. With default settings the saga times out and compensates after 30 s |
| The console shows `… unreachable` | That service isn't running on the port the console expects (`saga.ui.services.*`) |
| The stuck tile shows 0 although `stucksagas` lists one | The gauges refresh every 15 s by default (`saga.alert.refresh-interval`) |
| In Git Bash, `docker exec … /opt/kafka/bin/…` fails with a `C:/Program Files/Git/opt/...` path | Git Bash rewrites the path. Use CMD, or prefix the command with `MSYS_NO_PATHCONV=1` |
