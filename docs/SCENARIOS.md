# Scenarios: a hands-on lab

Fourteen scenarios, ordered from "see it work" to "break it and fix it". Each has the same parts:

> **Learn**: what the scenario teaches · **Do**: console steps and/or CMD commands · **See**: what to expect (real outputs) · **Why**: the code responsible · **Reset**: how to clean up, where needed

**Setup:** everything running as in [Getting started](GETTING-STARTED.md) (infrastructure, three services, console at http://localhost:8080).
- Scenarios 1–7 and 10 work with default settings.
- **Scenarios 8, 9, 11 and 12 need order-service started with fast timeouts**, otherwise you wait 30 s per step:
  ```
  start "order" java -jar order-service\target\order-service-0.0.1-SNAPSHOT.jar --saga.timeout.payment=10s --saga.timeout.inventory=10s --saga.timeout.compensation=10s --saga.alert.refresh-interval=2s
  ```

**Reading the console:**
- **State map.** Lit tracks are the route the saga took: cobalt for forward steps, amber for compensation. The thick-bordered box is the current state.
- **Message timeline.** One row per message, placed in the lane of the service that **sent** it ("to payment" means it was addressed to payment-service). `+ms` is the time since the order was created.

**Ids in commands.** Commands that need an order or saga id use `%ORDER%` / `%SAGA%`. Copy the id from the previous response and run `set ORDER=<id>` first. Customers and products are created with `PUT`, which overwrites, so every scenario can be re-run.

| # | Scenario | Teaches |
|---|---|---|
| 1 | [Happy path](#scenario-1--happy-path) | the basic flow, outbox, async completion |
| 2 | [Insufficient credit](#scenario-2--insufficient-credit) | failure with nothing to undo |
| 3 | [Out of stock → refund](#scenario-3--out-of-stock-compensation-by-refund) | compensation |
| 4 | [Ten orders race for the same stock](#scenario-4--ten-orders-race-for-the-same-stock) | locking, no overselling |
| 5 | [Duplicate deliveries are harmless](#scenario-5--duplicate-deliveries-are-harmless) | idempotency |
| 6 | [Poison messages and dead letters](#scenario-6--poison-messages-and-dead-letters) | retries, backoff, DLT |
| 7 | [Fix the cause, then replay](#scenario-7--fix-the-cause-then-replay-the-dead-letter) | DLT replay |
| 8 | [Payment timeout with a real outage](#scenario-8--payment-timeout-with-a-real-outage) | timeouts, late charge refunded |
| 9 | [Inventory timeout and release](#scenario-9--inventory-timeout-and-release) | multi-step compensation |
| 10 | [Fencing](#scenario-10--fencing-a-late-command-is-refused) | tombstones |
| 11 | [A compensation that won't finish](#scenario-11--a-compensation-that-wont-finish) | stuck detection, metrics, alerting |
| 12 | [Operator retry and resolve](#scenario-12--operator-retry-and-resolve) | manual resolution, audit |
| 13 | [Observability tour](#scenario-13--observability-tour) | outbox endpoint, metrics, alert-rule tests |
| 14 | [The automated suite](#scenario-14--the-automated-suite) | how the tests map to all of the above |

---

## Scenario 1 — Happy path

**Learn:** the basic orchestration flow. An order is accepted immediately and completes asynchronously through three local transactions.

**Do (console):** Saga tab → **Happy path**.

**Do (CMD):**
```
curl -X PUT http://localhost:8082/customers/alice -H "Content-Type: application/json" -d "{\"availableCredit\":500}"
curl -X PUT http://localhost:8083/products/widget -H "Content-Type: application/json" -d "{\"availableQuantity\":10}"
curl -i -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"alice\",\"productId\":\"widget\",\"quantity\":2,\"amount\":120.00}"
```

**See:**
- **The POST returns at once.** `HTTP/1.1 201`, `Location: /orders/<id>`, with `"status":"PENDING","sagaState":"PAYMENT_PENDING"`.
- **About a second later,** `curl http://localhost:8081/orders/%ORDER%` shows `"status":"APPROVED"` and `"sagaState":"COMPLETED"`.
- **Participant state:**
  - `curl http://localhost:8082/payments/%ORDER%` → `"status":"COMPLETED"`, amount 120.00
  - `curl http://localhost:8083/reservations/%ORDER%` → `"status":"RESERVED"`, quantity 2
  - `curl http://localhost:8082/customers/alice` → `380.00`
  - `curl http://localhost:8083/products/widget` → `8`
- **The four messages,** one outbox per service:
  ```
  curl http://localhost:8081/actuator/outbox/%ORDER%    → ProcessPayment, ReserveInventory   (commands)
  curl http://localhost:8082/actuator/outbox/%ORDER%    → PaymentProcessed                   (reply)
  curl http://localhost:8083/actuator/outbox/%ORDER%    → InventoryReserved                  (reply)
  ```
- **Console:** a straight cobalt route Payment pending → Inventory pending → Completed, and four timeline rows (e.g. +25 ms, +420 ms, +750 ms, +950 ms).
- **Logs:**
  - payment window: `Order <id> -> PaymentProcessed`
  - inventory window: `Order <id> -> InventoryReserved`
  - order window: `Saga <sagaId> for order <id> -> INVENTORY_PENDING`, then `… -> COMPLETED`

**Why:**
- [`OrderService.create`](../order-service/src/main/java/com/saga/order/api/OrderService.java) writes the order, the saga (with its deadline) and a `ProcessPayment` outbox row in one transaction.
- [`OutboxRelay`](../saga-common/src/main/java/com/saga/common/outbox/OutboxRelay.java) publishes the outbox row.
- [`PaymentCommandHandler`](../payment-service/src/main/java/com/saga/payment/PaymentCommandHandler.java) and [`InventoryCommandHandler`](../inventory-service/src/main/java/com/saga/inventory/InventoryCommandHandler.java) each do one transaction and write their reply to their own outbox.
- [`OrderSagaOrchestrator.onReply`](../order-service/src/main/java/com/saga/order/saga/OrderSagaOrchestrator.java) advances the state machine.

---

## Scenario 2 — Insufficient credit

**Learn:** a saga can fail before anything needs undoing; compensation is only for steps that actually happened.

**Do (console):** **Insufficient credit**.

**Do (CMD):**
```
curl -X PUT http://localhost:8082/customers/bob -H "Content-Type: application/json" -d "{\"availableCredit\":50}"
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"bob\",\"productId\":\"widget\",\"quantity\":1,\"amount\":200.00}"
```

**See:**
- `"status":"REJECTED"`, `"rejectionReason":"Insufficient credit"`, `"sagaState":"FAILED"`.
- `curl http://localhost:8082/payments/%ORDER%` → **404**: no charge was ever made.
- **Only two messages:** `ProcessPayment` (order) and `PaymentFailed` (payment). Inventory is never contacted.
- **Console:** one cobalt track, from Payment pending straight down to Failed.

**Why:** `PaymentCommandHandler.process` replies `PaymentFailed` instead of debiting. In the orchestrator, `PaymentFailed` in PAYMENT_PENDING goes directly to FAILED and rejects the order.

**Try also:** an unknown customer (`"customerId":"nobody"`) is rejected with `Unknown customer nobody`.

---

## Scenario 3 — Out of stock: compensation by refund

**Learn:** a **compensating transaction**. The charge already happened, so it's undone with a refund.

**Do (console):** **Out of stock**.

**Do (CMD):**
```
curl -X PUT http://localhost:8082/customers/carol -H "Content-Type: application/json" -d "{\"availableCredit\":500}"
curl -X PUT http://localhost:8083/products/gadget -H "Content-Type: application/json" -d "{\"availableQuantity\":0}"
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"carol\",\"productId\":\"gadget\",\"quantity\":1,\"amount\":80.00}"
```

**See:**
- `"status":"REJECTED"`, `"rejectionReason":"Insufficient stock"`, `"sagaState":"FAILED"`.
- `curl http://localhost:8082/payments/%ORDER%` → `"status":"REFUNDED"`. The charge happened and was reversed.
- `curl http://localhost:8082/customers/carol` → back to `500.00`.
- **Six messages, in this order:**

  | order (commands) | payment (replies) | inventory (replies) |
  |---|---|---|
  | ProcessPayment | | |
  | | PaymentProcessed | |
  | ReserveInventory | | |
  | | | InventoryFailed |
  | RefundPayment | | |
  | | PaymentRefunded | |

- **Console:** cobalt Payment pending → Inventory pending, then **amber** Inventory pending → Refunding payment → Failed. Note the rejection reason is the *original* failure ("Insufficient stock"), not "refunded".

**Why:** the orchestrator handles `InventoryFailed` by moving to COMPENSATING and sending `RefundPayment`. `PaymentCommandHandler.refund` credits the amount back and marks the payment REFUNDED. `PaymentRefunded` then moves the saga to FAILED.

---

## Scenario 4 — Ten orders race for the same stock

**Learn:** concurrency control. Pessimistic row locks stop overselling, and the losers are compensated.

**Do (console):** **Ten orders at once**. It creates a customer with 1000.00 credit and a product with 50 in stock, then fires ten orders of 10 units each at the same moment.

**Do (CMD):**
```
curl -X PUT http://localhost:8082/customers/dave -H "Content-Type: application/json" -d "{\"availableCredit\":1000}"
curl -X PUT http://localhost:8083/products/gizmo -H "Content-Type: application/json" -d "{\"availableQuantity\":50}"
for /L %i in (1,1,10) do start /b curl -s -o NUL -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"dave\",\"productId\":\"gizmo\",\"quantity\":10,\"amount\":20.00}"
```
(In a `.cmd` file write `%%i` instead of `%i`. If you re-run this, use a new customer id such as `dave2`, so the counts below only include this run.)

**See** (after a few seconds):
```
docker exec saga-postgres psql -U saga -d order_db -c "select status, saga.state, count(*) from orders o join order_saga saga on saga.order_id = o.id where o.customer_id = 'dave' group by 1, 2;"
```
```
  status  |   state   | count
----------+-----------+-------
 APPROVED | COMPLETED |     5
 REJECTED | FAILED    |     5
```
- Stock is `0`, never negative.
- Credit is `900.00`: 10 charges of 20, then 5 refunds.
- `payment_db`: 5 COMPLETED and 5 REFUNDED.
- `inventory_db`: 5 RESERVED, totalling exactly 50 units.

**Why:**
- `ProductRepository.findForUpdate` and `CustomerCreditRepository.findForUpdate` use `PESSIMISTIC_WRITE` (`SELECT … FOR UPDATE`), so concurrent reservations on one product run one at a time and each sees the stock left by the previous one.
- The five that find no stock reply `InventoryFailed` and are compensated as in Scenario 3.
- The `CHECK (available_quantity >= 0)` constraint is a last line of defense.

---

## Scenario 5 — Duplicate deliveries are harmless

**Learn:** at-least-once delivery means duplicates happen. This scenario shows the **two layers of idempotency**: message-level (`processed_message`) and business-key level (`payment.order_id`). It also shows the orchestrator's state guard dropping a reply that's no longer relevant.

**Do (CMD):** take the happy-path order from Scenario 1. You need its order id and saga id (`curl http://localhost:8081/orders/%ORDER%` shows both). Then send the **same** ProcessPayment twice, with the **same** `messageId`, using Kafka's console producer:
```
set ORDER=<order id>
set SAGA=<saga id>
for /L %i in (1,1,2) do @echo messageId:99999999-2222-3333-4444-555555555555,messageType:ProcessPayment;%ORDER%;{"sagaId":"%SAGA%","orderId":"%ORDER%","customerId":"alice","amount":120.00}| docker exec -i saga-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payment.commands --property parse.key=true --property parse.headers=true --property "headers.delimiter=;" --property "key.separator=;"
```
(The line format is `headers;key;value`. Headers are `name:value` pairs separated by commas. `;` is used as the delimiter because CMD doesn't need it escaped.)

**See:**
- **Only one of the two deliveries was processed:**
  ```
  docker exec saga-postgres psql -U saga -d payment_db -tAc "select count(*) from processed_message where message_id = '99999999-2222-3333-4444-555555555555'"
  ```
  → `1`. The second delivery found the id already recorded and was skipped.
- **No second charge:** `curl http://localhost:8082/customers/alice` is unchanged. The processed delivery found a COMPLETED payment for the order and replied `PaymentProcessed` without debiting (business-key idempotency).
- `curl http://localhost:8082/actuator/outbox/%ORDER%` has gained **exactly one** extra `PaymentProcessed`.
- **order window:** `Saga … in state COMPLETED ignoring PaymentProcessed (expected state PAYMENT_PENDING)`. The extra reply can't move a finished saga.

**Why:** [`IdempotencyGuard.firstDelivery`](../saga-common/src/main/java/com/saga/common/idempotency/IdempotencyGuard.java) inserts into `processed_message` with `ON CONFLICT DO NOTHING`, in the handler's own transaction. `PaymentCommandHandler.process` checks for an existing payment for the order first. `OrderSagaOrchestrator.expectedState` is the state guard.

---

## Scenario 6 — Poison messages and dead letters

**Learn:** the difference between a message that can **never** succeed (dead-lettered at once) and one that fails at runtime (retried with exponential backoff, then dead-lettered). And how to read a dead letter.

**Do (console):** Operations tab → **Inject a message**.
1. Preset **Unknown message type** → **Publish**.
2. Preset **Charge that crashes the handler** → **Publish**. This is a ProcessPayment with `"amount": null`; it throws a NullPointerException inside the handler.
3. Watch the **Dead-letter topics** panel. `payment.commands-dlt` goes up by 1 immediately, then by 1 more about 8 s later.

**Do (CMD)** for the first one:
```
curl -X POST http://localhost:8080/api/inject -H "Content-Type: application/json" -d "{\"topic\":\"payment.commands\",\"messageType\":\"NoSuchCommand\",\"payload\":\"{}\"}"
curl http://localhost:8082/actuator/dlt/payment.commands
```

**See** (payment window; real timings):
```
10:26:30 Dead-lettered payment.commands-1@3 to payment.commands-dlt: NonRetryableMessageException: Unknown message type 'NoSuchCommand' on payment.commands
10:26:30 Delivery attempt 1 failed for payment.commands-2@17: NullPointerException: Cannot read field "scale" because "val" is null
10:26:30 Delivery attempt 2 failed ...
10:26:31 Delivery attempt 3 failed ...
10:26:33 Delivery attempt 4 failed ...
10:26:37 Delivery attempt 5 failed ...
10:26:38 Dead-lettered payment.commands-2@17 to payment.commands-dlt: NullPointerException: ...
```
- **The unknown type:** one attempt, dead-lettered in the same second.
- **The crash:** 5 attempts with gaps of 0.5, 1, 2 and 4 s, then dead-lettered.

**Read the dead letters** with their headers. The easy way is AKHQ:
1. Open http://localhost:8086 → **Topics**. `payment.commands-dlt` now shows a count of 2.
2. Click it → **Data**, and set **Sort: NEWEST**.
3. Click the number in the **Headers** column of a record.

From the command line:
```
docker exec saga-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic payment.commands-dlt --from-beginning --property print.headers=true --property print.key=true --timeout-ms 5000
```
Each record keeps its original `messageId`, `messageType`, key and value. Spring adds:
- `kafka_dlt-exception-cause-fqcn` (e.g. `java.lang.NullPointerException`)
- `kafka_dlt-exception-message`
- `kafka_dlt-original-topic`
- a stack trace (on the command line the output is long; the useful part is at the start of each line)

**Why:**
- [`KafkaErrorHandlingConfig`](../saga-common/src/main/java/com/saga/common/kafka/KafkaErrorHandlingConfig.java) sets up the backoff (`saga.kafka.retry.*`) and marks `NonRetryableMessageException` and `JacksonException` as not retryable.
- [`MessageCodec.decode`](../saga-common/src/main/java/com/saga/common/messaging/MessageCodec.java) throws `NonRetryableMessageException` for an unknown type.
- The `Delivery attempt` and `Dead-lettered` lines come from its `LoggingRetryListener`.

**Try also:** the **Malformed JSON** preset (dead-lettered at once), and the **Reply for a saga that does not exist** preset (`order.saga.replies-dlt` on order-service).

---

## Scenario 7 — Fix the cause, then replay the dead letter

**Learn:** dead letters aren't lost. Once the cause is fixed, **replay** sends them back and they're processed normally.

**Story:** a reply arrives for a saga that order-service doesn't know (yet). We'll fix that by creating the saga, then replay.

**Do (CMD):**
```
set ORDER=aaaaaaaa-0000-0000-0000-000000000007
set SAGA=bbbbbbbb-0000-0000-0000-000000000007

curl -X POST http://localhost:8080/api/inject -H "Content-Type: application/json" -d "{\"topic\":\"order.saga.replies\",\"key\":\"%ORDER%\",\"messageType\":\"PaymentProcessed\",\"payload\":\"{\\\"sagaId\\\":\\\"%SAGA%\\\",\\\"orderId\\\":\\\"%ORDER%\\\"}\"}"
curl http://localhost:8081/actuator/dlt/order.saga.replies
```
→ `"pending":1`. The order window logs `Dead-lettered order.saga.replies-… : NonRetryableMessageException: Unknown saga bbbbbbbb-…`.

Fix the cause by creating the order and its saga, waiting for exactly that reply:
```
docker exec saga-postgres psql -U saga -d order_db -c "insert into orders (id, customer_id, product_id, quantity, amount, status, version, created_at, updated_at) values ('%ORDER%', 'alice', 'widget', 1, 10.00, 'PENDING', 0, now(), now()); insert into order_saga (id, order_id, state, version, created_at, updated_at) values ('%SAGA%', '%ORDER%', 'PAYMENT_PENDING', 0, now(), now());"
```
Replay (or press **Replay** on the `order.saga.replies-dlt` card in the console):
```
curl -X POST http://localhost:8081/actuator/dlt/order.saga.replies -H "Content-Type: application/json" -d "{}"
```
→ `{"topic":"order.saga.replies","dltTopic":"order.saga.replies-dlt","replayed":1,"pending":0}`

**See:** `curl http://localhost:8081/orders/%ORDER%` → `"status":"APPROVED"`, `"sagaState":"COMPLETED"`. The replayed `PaymentProcessed` advanced the saga, and the rest of the flow ran normally.

**Also:** replay the payment DLT from Scenario 6 (`POST http://localhost:8082/actuator/dlt/payment.commands`). Those messages are still broken, so they fail again and come back to the DLT. Read it again as in Scenario 6 and you'll see a `dltReplayCount:1` header.

**Why:** [`DltReplayer`](../saga-common/src/main/java/com/saga/common/kafka/dlt/DltReplayer.java) reads from the committed offset of the group `order-service-dlt-replay` up to a snapshot of the end offset. It re-publishes each record to the source topic (dropping `kafka_dlt-*` headers, incrementing `dltReplayCount`), then commits. Replaying twice never re-sends the same dead letter, and even if it did, idempotency (Scenario 5) would absorb it. [`DltEndpoint`](../saga-common/src/main/java/com/saga/common/kafka/dlt/DltEndpoint.java) only allows topics this service consumes.

---

## Scenario 8 — Payment timeout with a real outage

*Needs the fast-timeout order-service (see the top of this page).*

**Learn:** a participant outage doesn't hang the saga, and a charge that happens **late** is still undone, because the compensation is queued behind it on the same partition.

**Do:**
1. Close the **payment** window. Its health dot in the console turns red.
2. Place an order:
   ```
   curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"alice\",\"productId\":\"widget\",\"quantity\":1,\"amount\":30.00}"
   ```
3. Watch it in the console (or poll `curl http://localhost:8081/orders/%ORDER%`):

   | after | sagaState | order window |
   |---|---|---|
   | 5 s | PAYMENT_PENDING | |
   | ~10 s | **COMPENSATING** | `Saga … timed out -> COMPENSATING` |
   | every 10 s | COMPENSATING | `… timed out -> COMPENSATING` (the refund is re-sent) |

4. Start payment-service again:
   ```
   start "payment" java -jar payment-service\target\payment-service-0.0.1-SNAPSHOT.jar
   ```

**See:** within a second of payment-service starting, the payment window shows `Order … -> PaymentProcessed`, then several `Order … -> PaymentRefunded`.
- The **late charge** happened first: ProcessPayment was waiting in Kafka.
- The first refund behind it **undid** it; the extra re-sent refunds were no-ops.

The result:
- **Order:** `"status":"REJECTED"`, `"rejectionReason":"Payment timed out"`, `"sagaState":"FAILED"`.
- **Payment:** `curl http://localhost:8082/payments/%ORDER%` → `"status":"REFUNDED"`.
- **Credit:** alice's credit is the same as before the order.
- **order window:** `ignoring PaymentProcessed (expected state PAYMENT_PENDING)`, the late success reply, dropped.
- **Console:** amber track Payment pending → Refunding payment ("payment timeout") → Failed. The timeline shows one ProcessPayment followed by many RefundPayment rows.

**Why:**
- [`SagaTimeoutScanner`](../order-service/src/main/java/com/saga/order/saga/SagaTimeoutScanner.java) finds overdue sagas.
- `OrderSagaOrchestrator.onTimeout` compensates without asking whether the charge happened. Ordering by key (both messages use the orderId) guarantees the refund is processed after the charge.
- The refund is idempotent.

---

## Scenario 9 — Inventory timeout and release

*Needs the fast-timeout order-service.*

**Learn:** a two-step compensation. Release the (possibly late) reservation, then refund.

**Do:**
1. Make sure widget has stock (`curl -X PUT http://localhost:8083/products/widget -H "Content-Type: application/json" -d "{\"availableQuantity\":10}"`), then close the **inventory** window.
2. Place an order:
   ```
   curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"customerId\":\"alice\",\"productId\":\"widget\",\"quantity\":3,\"amount\":60.00}"
   ```
3. Within a few seconds the saga is INVENTORY_PENDING and alice has been charged. After about 10 s it moves to **RELEASING_INVENTORY**, and ReleaseInventory is re-sent every 10 s.
4. Start inventory-service again: `start "inventory" java -jar inventory-service\target\inventory-service-0.0.1-SNAPSHOT.jar`.

**See:**
- **Order:** `"status":"REJECTED"`, `"rejectionReason":"Inventory timed out"`.
- **Reservation:** `curl http://localhost:8083/reservations/%ORDER%` → `"status":"RELEASED"`. The late reservation was made, then released.
- **Stock and payment:** stock is back to `10`, the payment is `REFUNDED`, and alice's credit is back where it started.
- **Messages:**
  - order: `ProcessPayment, ReserveInventory, ReleaseInventory ×N, RefundPayment`
  - inventory: `InventoryReserved, InventoryReleased ×N`
  - payment: `PaymentProcessed, PaymentRefunded`
- **Console:** cobalt to Inventory pending, then amber Inventory pending → Releasing inventory → Refunding payment → Failed.

**Why:** the timeout in INVENTORY_PENDING sends `ReleaseInventory`. [`InventoryCommandHandler.release`](../inventory-service/src/main/java/com/saga/inventory/InventoryCommandHandler.java) returns the stock if a reservation exists. Only after `InventoryReleased` does the orchestrator start the refund.

---

## Scenario 10 — Fencing: a late command is refused

**Learn:** **tombstones**. When a compensation finds nothing to undo, it leaves a marker so the original command, if it ever turns up (a replay, a very slow retry), is refused.

**Do (CMD):** create a saga waiting for inventory, deliver the **release first** and the **reserve after**:
```
set ORDER=cccccccc-0000-0000-0000-000000000010
set SAGA=dddddddd-0000-0000-0000-000000000010
docker exec saga-postgres psql -U saga -d order_db -c "insert into orders (id, customer_id, product_id, quantity, amount, status, version, created_at, updated_at) values ('%ORDER%', 'alice', 'widget', 1, 10.00, 'PENDING', 0, now(), now()); insert into order_saga (id, order_id, state, version, created_at, updated_at) values ('%SAGA%', '%ORDER%', 'INVENTORY_PENDING', 0, now(), now());"

curl -X POST http://localhost:8080/api/inject -H "Content-Type: application/json" -d "{\"topic\":\"inventory.commands\",\"key\":\"%ORDER%\",\"messageType\":\"ReleaseInventory\",\"payload\":\"{\\\"sagaId\\\":\\\"%SAGA%\\\",\\\"orderId\\\":\\\"%ORDER%\\\",\\\"productId\\\":\\\"widget\\\",\\\"quantity\\\":1}\"}"
curl -X POST http://localhost:8080/api/inject -H "Content-Type: application/json" -d "{\"topic\":\"inventory.commands\",\"key\":\"%ORDER%\",\"messageType\":\"ReserveInventory\",\"payload\":\"{\\\"sagaId\\\":\\\"%SAGA%\\\",\\\"orderId\\\":\\\"%ORDER%\\\",\\\"productId\\\":\\\"widget\\\",\\\"quantity\\\":1}\"}"
```

**See:**
- **The tombstone:** `curl http://localhost:8083/reservations/%ORDER%` → `"status":"RELEASED"`, quantity 1. The release found nothing and recorded one.
- **No stock taken:** `curl http://localhost:8083/products/widget` is unchanged. The late reserve was refused.
- **Order:** `"status":"REJECTED"`, `"rejectionReason":"Order already released"`. The refusal (`InventoryFailed`) made the saga compensate.
- **Payment:** `curl http://localhost:8082/payments/%ORDER%` → `"status":"CANCELLED"`. That refund found no payment either, so it left a payment tombstone, fencing off any late ProcessPayment for this order too.

**Why:**
- `InventoryCommandHandler.release` inserts `Reservation.tombstone(...)`, and `reserve` refuses when a RELEASED row exists.
- `PaymentCommandHandler.refund` inserts `Payment.cancelled(...)`, and `process` refuses CANCELLED or REFUNDED.

---

## Scenario 11 — A compensation that won't finish

*Needs the fast-timeout order-service (with `--saga.alert.refresh-interval=2s`).*

**Learn:** compensations retry forever by design, so the system must **tell someone** when one isn't finishing.

**Do:** as Scenario 8 steps 1–3 (payment window closed, place an order), but **don't restart payment-service** yet. Open the console's **Operations** tab.

**See** (with 10 s timeouts):

| after | what happens |
|---|---|
| ~10 s | saga COMPENSATING (payment timed out) |
| ~20, 30, 40 s | refund re-sent; `compensation_resends` 1, 2, 3 |
| ~40 s | 3rd re-send reaches the threshold (`saga.alert.stuck-after-resends=3`) |

- **order window:** `STUCK COMPENSATION: saga … for order … in COMPENSATING has re-sent its compensation 3 times (compensating since …, reason: Payment timed out). See GET /actuator/stucksagas`. After that, one `still stuck … after N compensation re-sends` line per re-send.
- **The stuck list:**
  ```
  curl http://localhost:8081/actuator/stucksagas
  ```
  ```
  [{"sagaId":"…","orderId":"…","state":"COMPENSATING","failureReason":"Payment timed out","compensationResends":3,"compensatingSince":"…","deadline":"…"}]
  ```
- **The gauge:** `curl http://localhost:8081/actuator/metrics/saga.compensation.stuck` → `"value":1.0`.
- **Console:** the **Stuck sagas** tile turns red and the saga is listed.
- **Prometheus:** `curl http://localhost:8081/actuator/prometheus | findstr saga_compensation` shows `saga_compensation_stuck 1.0`. That is what the `SagaCompensationStuck` alert in [`ops/prometheus/saga-alerts.yml`](../ops/prometheus/saga-alerts.yml) watches.

**Recover:** either restart payment-service (it recovers as in Scenario 8, and the saga drops off the list), or go on to Scenario 12 with the saga still stuck.

**Why:** `OrderSaga.resendCompensation` counts re-sends. [`CompensationMonitor`](../order-service/src/main/java/com/saga/order/saga/CompensationMonitor.java) logs at the threshold (after commit only) and refreshes the gauges. [`StuckSagasEndpoint`](../order-service/src/main/java/com/saga/order/saga/StuckSagasEndpoint.java) lists the sagas.

---

## Scenario 12 — Operator retry and resolve

*Continue from Scenario 11: payment-service still down, one stuck saga.*

**Learn:** the two operator actions, why `resolve` needs a manual compensation first, and the audit trail. The [Runbook](../ops/RUNBOOK.md) is the production version of this.

**Do (console):** Operations → **Open order** on the stuck saga. In the Saga card:
1. Enter your name → **Retry compensation**. The re-send count drops to 0 (the refund was re-sent now). Payment is still down, so it will become stuck again after 3 more timeouts.
2. Click **Resolve as failed** without a note. It's refused: *note is required for resolve*.

**Do (CMD)** for the same steps, with the saga id from `/actuator/stucksagas`:
```
set SAGA=<saga id>
curl -X POST http://localhost:8081/actuator/stucksagas/%SAGA% -H "Content-Type: application/json" -d "{\"action\":\"retry\",\"operator\":\"alice-oncall\"}"
```
→ `"action":"RETRY","fromState":"COMPENSATING","toState":"COMPENSATING","compensationResends":0`

**Compensate by hand, then resolve.** Payment-service never charged this order (it was down), so the correct manual compensation is a **CANCELLED tombstone**: it fences off the queued ProcessPayment.
```
set ORDER=<order id>
docker exec saga-postgres psql -U saga -d payment_db -c "insert into payment (id, order_id, status, created_at, updated_at) values (gen_random_uuid(), '%ORDER%', 'CANCELLED', now(), now());"
curl -X POST http://localhost:8081/actuator/stucksagas/%SAGA% -H "Content-Type: application/json" -d "{\"action\":\"resolve\",\"operator\":\"alice-oncall\",\"note\":\"Payment-service down; nothing charged; CANCELLED tombstone inserted, ticket OPS-42\"}"
```
→ `"toState":"FAILED","orderStatus":"REJECTED"`. The order now reads `"rejectionReason":"Payment timed out (compensation resolved manually)"`.

Now start payment-service again.

**See:**
- **The queued charge is refused:** the payment outbox shows `PaymentFailed` with `"reason":"Order already CANCELLED"`. The queued refunds are no-ops (`PaymentRefunded`).
- **No money moved:** alice's credit is unchanged, and `curl http://localhost:8082/payments/%ORDER%` → `"status":"CANCELLED"`.
- **order window:** `Saga … in state FAILED ignoring PaymentFailed …` and `… ignoring PaymentRefunded …`. The saga is closed and late replies can't reopen it.
- **The audit trail:** `curl http://localhost:8081/actuator/stucksagas/%SAGA%` → `"interventions":[{"action":"RETRY","operator":"alice-oncall",…},{"action":"RESOLVE",…,"note":"Payment-service down; …"}]`. The console shows both in the saga card and the timeline.

**The rule this demonstrates:** *compensate by hand **in the participant's own records** before resolving.* If the payment had been charged, you would mark it REFUNDED and restore the credit instead; the [Runbook](../ops/RUNBOOK.md#3b-cannot-be-fixed-in-time--compensate-by-hand-then-resolve) has the SQL. Either way, anything still queued becomes a no-op, so there's no double refund. The e2e test `resolve_afterManualRefund_closesSaga_andLateCompensationDoesNotRefundTwice` proves the charged variant.

**Why:** [`SagaInterventionService.intervene`](../order-service/src/main/java/com/saga/order/saga/intervention/SagaInterventionService.java) (optimistic lock, audit row in `saga_intervention`, WARN log `Operator … applied …`).

---

## Scenario 13 — Observability tour

**Learn:** where to look when something is off.

| Question | Look at |
|---|---|
| What happened to this order? | Console order detail, or `GET /actuator/outbox/{orderId}` on all three services (commands from order-service, replies from the participants) |
| What is actually on the topics? | AKHQ at http://localhost:8086: messages, headers, Live Tail, search by key (orderId) ([Getting started §8](GETTING-STARTED.md#kafka-in-the-browser-akhq)) |
| Is a message stuck? | `GET /actuator/dlt` on each service. Kafka lag: AKHQ's **Consumer Groups**, or `kafka-consumer-groups.sh --describe --group <service>` ([Getting started §8](GETTING-STARTED.md#kafka-from-the-command-line)) |
| Is any compensation stuck? | `GET /actuator/stucksagas`, the gauge `saga.compensation.stuck`, the log line `STUCK COMPENSATION` |
| Which metrics exist? | `curl http://localhost:8081/actuator/metrics`, and `curl http://localhost:8081/actuator/prometheus \| findstr saga_` |
| Will the alerts fire correctly? | Run the promtool tests: `docker run --rm -v "%cd%\ops\prometheus":/rules --entrypoint promtool prom/prometheus test rules /rules/saga-alerts.test.yml` → `SUCCESS` |
| Is a service healthy? | `GET /actuator/health`, or the dots in the console header |

Try this: open the **API console** tab. Click through the catalogue entries for every service; each fills in method, path and body. Send each one and compare the responses with what you've seen in the scenarios.

---

## Scenario 14 — The automated suite

**Learn:** everything above is also proven automatically, on every `mvn clean verify`.

```
mvn clean verify
```
(Stop your local services first. The suite brings up its own Postgres and Kafka with Testcontainers and runs the four real jars. It takes about 2 minutes.)

To speed things up, the tests run the services with 1 s/2 s/4 s backoff, 3 s step timeouts, a stuck threshold of 2, and 500 ms gauge refresh. Logs from the run are in `saga-e2e\target\e2e-logs\`.

| Test in [`SagaEndToEndIT`](../saga-e2e/src/test/java/com/saga/e2e/SagaEndToEndIT.java) | Scenario |
|---|---|
| `happyPath_approvesOrder_debitsCreditAndReservesStock` | 1 |
| `insufficientCredit_rejectsOrder_andNeverTouchesInventory`, `unknownCustomer_rejectsOrder` | 2 |
| `outOfStock_compensatesWithRefund_andRejectsOrder` | 3 |
| `concurrentOrders_neverOversellStock_andRefundTheLosers` | 4 |
| `unknownMessageType_skipsRetries_andIsDeadLetteredImmediately`, `transientHandlerFailure_isRetriedWithBackoff_thenDeadLettered`, `replyForUnknownSaga_isDeadLettered_andSagaFlowKeepsWorking` | 6 |
| `replay_reprocessesDeadLetter_onceTheCauseIsFixed`, `replay_ofStillBrokenMessage_landsBackOnDlt_withReplayCount`, `dltEndpoint_onlyServesTopicsTheServiceConsumes` | 7 |
| `paymentTimeout_refundsTheLateCharge_andRejectsOrder` (stalls payment with poison messages instead of stopping it) | 8 |
| `inventoryTimeout_releasesTheLateReservation_refunds_andRejectsOrder` | 9 |
| `refundBeforeCharge_leavesTombstone_andRefusesTheLateCharge`, `releaseBeforeReserve_leavesTombstone_andRefusesTheLateReservation` | 10 |
| `failingRefund_isReportedAsStuck_andClearsOnceTheRefundSucceeds` (a DB trigger makes refunds fail) | 11 |
| `retry_resendsCompensation_resetsStuckCount_andIsAudited`, `resolve_afterManualRefund_closesSaga_andLateCompensationDoesNotRefundTwice`, `intervention_rejectsBadRequests` | 12 |
| `readApis_exposeParticipantState_andOutboxesFormTheTimeline`, `seedApis_upsertAndValidate`, `sagaConsole_servesPage_proxiesServices_andInjectsMessages` | 13 and the console |

Duplicate delivery (Scenario 5) is covered implicitly. Every compensation re-send and every replay in the suite relies on it.

Next: the [Codebase tour](CODEBASE.md) shows where each piece lives and how to extend it.
