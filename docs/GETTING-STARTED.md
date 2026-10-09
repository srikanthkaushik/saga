# Getting started

Everything here is for **Windows Command Prompt (CMD)**. All commands run from the repository root (`C:\DEVL\SAGA` or wherever you cloned it) unless stated otherwise.

- [1. Prerequisites](#1-prerequisites)
- [2. Build](#2-build)
- [3. Start everything](#3-start-everything)
- [4. Sign in: users, roles and tokens](#4-sign-in-users-roles-and-tokens)
- [5. Your first order](#5-your-first-order)
- [6. Configuration overrides](#6-configuration-overrides)
- [7. Stop and reset](#7-stop-and-reset)
- [8. Run the tests](#8-run-the-tests)
- [9. Look inside: databases and Kafka](#9-look-inside-databases-and-kafka)
- [10. Troubleshooting](#10-troubleshooting)

## 1. Prerequisites

| Tool | Version | Check |
|---|---|---|
| JDK | 21 | `java -version` |
| Maven | 3.9+ | `mvn -v` (must report Java 21) |
| Docker Desktop | any recent, **running** | `docker version` (both Client and Server sections) |
| curl | ships with Windows 10+ | `curl --version` |
| A browser | – | for the console at http://localhost:8080 |

Ports that must be free: **5432** (Postgres), **9092** (Kafka), **8080–8083** (console and services), **8086** (AKHQ Kafka UI), **8180** (Keycloak). Check one with `netstat -ano | findstr :8081`.

## 2. Build

```
mvn clean install -DskipITs
```

This builds all six modules in about 10 s, skipping the end-to-end suite (which needs Docker and takes a few minutes, see [section 8](#8-run-the-tests)). The runnable jars land in:

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
This starts four containers:

| Container | What | Reach it at |
|---|---|---|
| `saga-postgres` | Postgres 17, user `saga` / password `saga` | `localhost:5432` |
| `saga-kafka` | Kafka 4.0, KRaft mode, a single broker | `localhost:9092` from the host; `kafka:29092` from other containers |
| `saga-akhq` | [AKHQ](https://akhq.io) 0.28.0, a web UI for Kafka | http://localhost:8086 |
| `saga-keycloak` | [Keycloak](https://www.keycloak.org) 26.7, the identity provider: sign-in, roles, tokens | http://localhost:8180 (admin console: `admin` / `admin`) |

On first start, `docker\postgres\init.sql` creates the three databases `order_db`, `payment_db` and `inventory_db`. Data survives container re-creation, `docker compose down` and restarts. It lives in two named volumes:

| Volume | Holds |
|---|---|
| `saga_saga-postgres-data` | the three databases |
| `saga_saga-kafka-data` | Kafka's topics, messages and committed consumer offsets |

Only `docker compose down -v` deletes them.

Kafka's storage is formatted on first start with the `CLUSTER_ID` set in `docker-compose.yml`. **Don't change that id while the volume exists:** Kafka refuses to start on storage formatted for a different cluster. If you need a new id, wipe first ([section 7](#7-stop-and-reset)).

Kafka has two client listeners because "localhost" means something different in each place:
- `localhost:9092` for the services on your machine;
- `kafka:29092` on the compose network, for AKHQ.

A container told to use `localhost:9092` would try to connect to itself.

Keycloak runs in development mode and keeps no state of its own: on every start it imports the `saga` realm from `docker\keycloak\saga-realm.json` (roles, clients, dev users). Keycloak takes about 20 s to start. **Start it before the console**, which looks up Keycloak when it starts. The services only need it when the first token arrives.

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
The browser is sent to Keycloak to sign in ([section 4](#4-sign-in-users-roles-and-tokens)). Afterwards the header shows who you are and a **Sign out** link, and the three dots show service health (green = up).

### 3.4 Seed data
The Flyway migrations create these:

| Id | Value | Purpose |
|---|---|---|
| `customer-1` | 1000.00 credit | general use |
| `customer-2` | 50.00 credit | too little for most orders |
| `product-1` | 100 in stock | general use |
| `product-2` | 0 in stock | always fails inventory, so it triggers a refund |

With an admin token ([section 4](#4-sign-in-users-roles-and-tokens)) you can create or overwrite any customer or product at any time:
```
call ops\token.cmd admin admin
curl -H "Authorization: Bearer %TOKEN%" -X PUT http://localhost:8082/customers/alice -H "Content-Type: application/json" -d "{\"availableCredit\":500}"
curl -H "Authorization: Bearer %TOKEN%" -X PUT http://localhost:8083/products/widget -H "Content-Type: application/json" -d "{\"availableQuantity\":10}"
```

## 4. Sign in: users, roles and tokens

Placing and reading an order (`POST /orders`, `GET /orders/{id}`) and health checks are public. **Everything else needs a Keycloak token**: actuator endpoints (DLT, stuck sagas, outbox, metrics), the read APIs for customers, products, payments and reservations, the order list, and the seed `PUT`s. The console needs a login.

**Development users** (password = username):

| User | Roles | Can |
|---|---|---|
| `viewer` | saga-viewer | read actuator endpoints, participant state, the order list |
| `operator` | saga-operator (+ viewer) | also replay DLTs, retry/resolve stuck sagas, inject messages in the console |
| `admin` | saga-admin (+ operator, viewer) | also set customer credit and product stock (the console's scenarios need this) |

There is also a client `saga-prometheus` (client-credentials grant) with only `saga-metrics`, for scraping `/actuator/prometheus`. The roles are composite in Keycloak, so a token lists every role its user effectively has.

**In the console:** sign in with one of the users above. Buttons your roles can't use are disabled, with the reason shown. Interventions are recorded under your user name.

**With curl:** get a token into `%TOKEN%` with the helper, then send it as a bearer token:
```
call ops\token.cmd operator operator
curl -H "Authorization: Bearer %TOKEN%" http://localhost:8081/actuator/stucksagas
```
- **Expiry:** tokens last **5 minutes**. When you get a 401, run `call ops\token.cmd …` again.
- **401 vs 403:** 401 means no token, an invalid token or an expired one. 403 means a valid token whose user lacks the role.
- **How the helper works:** it uses the `saga-cli` client with the password grant. That's convenient for local development only, which is why the client exists only in the dev realm.

Which role each endpoint needs is listed in [PROJECT.md](../PROJECT.md#rest-apis).

## 5. Your first order

```
curl -i -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"customer-1\",\"productId\":\"product-1\",\"quantity\":2,\"amount\":100.00}"
```
The response is `201 Created`, with `"status":"PENDING"` and `"sagaState":"PAYMENT_PENDING"`. The saga runs asynchronously. About a second later:
```
curl http://localhost:8081/orders/<id from the response>
```
shows `"status":"APPROVED"` and `"sagaState":"COMPLETED"`.

Now go to [Scenarios](SCENARIOS.md) and work through them in order.

## 6. Configuration overrides

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

## 7. Stop and reset

| Goal | Do |
|---|---|
| Stop a service | Close its window, or press Ctrl+C in it |
| Stop infrastructure, keep data | `docker compose down`. Databases, topics, messages and consumer offsets all survive |
| **Wipe everything** (databases and Kafka) | `docker compose down -v`, then `docker compose up -d` and restart the services so Flyway recreates the schema and the services recreate their topics |

Wipe Postgres and Kafka **together**. If you clear only one, the other still refers to sagas that no longer exist. For example, replies left in Kafka for sagas missing from `order_db` would be dead-lettered as "Unknown saga".

## 8. Run the tests

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

## 9. Look inside: databases and Kafka

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

### Kafka in the browser: AKHQ
Open **http://localhost:8086** (cluster `saga-local`).

| To see | In AKHQ |
|---|---|
| All topics with message counts, size, last record and the lag of each consuming group | **Topics** (the start page) |
| The messages on a topic, newest first, with key, partition and offset | Click a topic → **Data**. Set **Sort: NEWEST** |
| A message's headers (`messageId`, `messageType`, and for dead letters every `kafka_dlt-*` header) | In **Data**, click the number in the **Headers** column |
| Messages for one order | **Data** → **Search** by key: the orderId |
| Messages as they arrive | **Live Tail** (bottom of a topic page) |
| Consumer groups, members (3 per service, one per partition) and lag per partition | **Consumer Groups** |

AKHQ here is **read-write and unauthenticated**. It can also produce, copy and empty topics, and reset consumer-group offsets. That's handy locally (e.g. emptying a DLT after you've dealt with it), but don't expose it beyond your machine. To re-send dead letters, prefer the services' own `dlt` replay endpoint ([Scenario 7](SCENARIOS.md#scenario-7--fix-the-cause-then-replay-the-dead-letter)): it strips the `kafka_dlt-*` headers and tracks what was already replayed.

### Kafka from the command line
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

## 10. Troubleshooting

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
| AKHQ at http://localhost:8086 shows no cluster, or times out | `docker compose ps` should show `saga-akhq` as healthy, and `docker logs saga-akhq` should not show connection errors. AKHQ must reach Kafka at `kafka:29092` (the `DOCKER` listener), not `localhost:9092` |
| Topics in AKHQ are empty | Kafka was wiped (`docker compose down -v`) and the services haven't been started since. Starting them recreates their topics |
| Kafka won't start and logs `Invalid cluster.id` | `CLUSTER_ID` in `docker-compose.yml` no longer matches the formatted storage. Restore the old id, or wipe with `docker compose down -v` |
| `DUPLICATE_BROKER_REGISTRATION` INFO lines right after Kafka restarts | The controller still holds the previous incarnation's session for a few seconds. Harmless; it registers on its own |
| A JWT "SECURITY WARNING" banner in `docker logs saga-akhq` | AKHQ's notice that it runs without authentication. Expected locally |
| A console scenario says `…-service is down, so this scenario can't create its …` | Scenarios create a fresh customer and product first, which needs payment- and inventory-service. To watch an outage, use **Place an order** with an existing customer and product (e.g. `customer-1`, `product-1`) instead |
| `401` from a service | No token, an expired one (they last 5 minutes), or one from another issuer. Run `call ops\token.cmd <user> <password>` again, and use `http://localhost:8180` for Keycloak (not `127.0.0.1`), because the URL becomes the token's issuer |
| `403` from a service | The token is valid, but its user lacks the role (e.g. a viewer replaying a DLT, an operator setting stock). See the role table in [§4](#4-sign-in-users-roles-and-tokens) |
| The console fails to start with `Unable to resolve Configuration with the provided Issuer` | Keycloak isn't up yet. `docker compose up -d`, wait about 20 s until http://localhost:8180/realms/saga/.well-known/openid-configuration answers, then start the console |
| Keycloak says `Invalid parameter: redirect_uri` | The console isn't at http://localhost:8080 (the only redirect URI the dev realm allows). Use port 8080, or add your URL to the `saga-console` client |
| `call ops\token.cmd` prints `Keycloak refused: invalid_grant` | Wrong user or password. Dev users are `viewer`, `operator` and `admin`, with password = username |
| In Git Bash, `docker exec … /opt/kafka/bin/…` fails with a `C:/Program Files/Git/opt/...` path | Git Bash rewrites the path. Use CMD, or prefix the command with `MSYS_NO_PATHCONV=1` |
