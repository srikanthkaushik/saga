# SAGA operator runbook

Commands are for Windows CMD against local ports: order 8081, payment 8082, inventory 8083. Replace the hosts for other environments.

> Admin endpoints need a Keycloak token: `call ops\token.cmd <user> <password>`, then `-H "Authorization: Bearer %TOKEN%"`. **Reading** needs `saga-viewer`; **replay, retry and resolve** need `saga-operator`; seed `PUT`s need `saga-admin`. The audit records the token's user, not anything in the request body. Tokens last 5 minutes. Local dev users: `viewer`, `operator`, `admin` (password = username).

> Everything below can also be done from the saga console (`java -jar saga-ui\target\saga-ui-0.0.1-SNAPSHOT.jar`, http://localhost:8080): **Operations** lists stuck sagas and DLTs with replay, and opening an order offers Retry and Resolve.

---

## SagaCompensationStuck (page)

**Meaning:** at least one saga keeps re-sending a refund (`COMPENSATING`) or an inventory release (`RELEASING_INVENTORY`) without getting a reply. Until it's fixed, the customer may be **charged for a rejected order**, or stock may be **held** for it.

### 1. Find the sagas
```
curl -H "Authorization: Bearer %TOKEN%" http://localhost:8081/actuator/stucksagas
```
Each entry shows `sagaId`, `orderId`, `state`, `failureReason`, `compensationResends` and `compensatingSince`.
- `COMPENSATING`: the refund is not completing. Look at **payment-service**.
- `RELEASING_INVENTORY`: the release is not completing. Look at **inventory-service**.

History for one saga, including earlier interventions:
```
curl -H "Authorization: Bearer %TOKEN%" http://localhost:8081/actuator/stucksagas/<sagaId>
```

### 2. Find the cause
1. **Is the participant up?** `curl http://localhost:8082/actuator/health` (payment) or `:8083` (inventory).
2. **Are its compensations dead-lettering?**
   ```
   curl -H "Authorization: Bearer %TOKEN%" http://localhost:8082/actuator/dlt/payment.commands
   curl -H "Authorization: Bearer %TOKEN%" http://localhost:8083/actuator/dlt/inventory.commands
   ```
   If `pending` is greater than 0, the participant's log has a `Dead-lettered payment.commands-…` ERROR line with the root cause. Every failed attempt also logs a `Delivery attempt N failed …` WARN.
3. **Is the reply getting back?** Look for `ignoring PaymentRefunded` / `ignoring InventoryReleased` WARN lines in the order-service log. If there are none and the participant shows the refund done, check that the participant's outbox is publishing: its `outbox` table should have no old rows with `published_at is null`.

### 3a. Cause fixed → retry (preferred)
Once the participant is healthy again (redeployed, data fixed, DLT replayed):
```
curl -H "Authorization: Bearer %TOKEN%" -X POST http://localhost:8081/actuator/stucksagas/<sagaId> -H "Content-Type: application/json" -d "{\"action\":\"retry\"}"
```
This re-sends the compensation now and resets the stuck count. Check that the saga reaches `FAILED` and the order `REJECTED`:
```
curl http://localhost:8081/orders/<orderId>
```
If the compensations had been dead-lettered, you can replay them instead of retrying, or as well; duplicates are harmless:
```
curl -H "Authorization: Bearer %TOKEN%" -X POST http://localhost:8082/actuator/dlt/payment.commands -H "Content-Type: application/json" -d "{}"
```

### 3b. Cannot be fixed in time → compensate by hand, then resolve
Only when the participant can't process the compensation (for example, corrupted data needing a manual fix).

**Rule: do the compensation in the participant's own database, never only outside it.** Then any compensation still queued, or replayed later, finds the work done and is a no-op. A refund done only outside the system (for example, at the bank) would be repeated when the refund command later succeeds: a **double refund**.

Refund by hand (payment_db), in one transaction:
```sql
begin;
update customer_credit cc set available_credit = cc.available_credit + p.amount
  from payment p
 where p.order_id = '<orderId>' and p.status = 'COMPLETED' and cc.customer_id = p.customer_id;
update payment set status = 'REFUNDED', updated_at = now()
 where order_id = '<orderId>' and status = 'COMPLETED';
-- each update must report exactly 1 row; otherwise ROLLBACK and investigate
commit;
```
If there is **no** payment row for the order, nothing was charged. Insert a tombstone so a late charge is refused:
```sql
insert into payment (id, order_id, status, created_at, updated_at)
values (gen_random_uuid(), '<orderId>', 'CANCELLED', now(), now());
```

Release by hand (inventory_db):
```sql
begin;
update product pr set available_quantity = pr.available_quantity + r.quantity
  from reservation r
 where r.order_id = '<orderId>' and r.status = 'RESERVED' and pr.product_id = r.product_id;
update reservation set status = 'RELEASED', released_at = now()
 where order_id = '<orderId>' and status = 'RESERVED';
commit;
```
If there is no reservation row, insert a tombstone (`status = 'RELEASED'`, `released_at = now()`, with the order's product_id and quantity).

For a `RELEASING_INVENTORY` saga, release by hand **and** refund by hand: resolving skips both remaining steps.

Then resolve. The note is mandatory; say what you did and where:
```
curl -H "Authorization: Bearer %TOKEN%" -X POST http://localhost:8081/actuator/stucksagas/<sagaId> -H "Content-Type: application/json" -d "{\"action\":\"resolve\",\"note\":\"Refunded in payment_db, ticket <id>\"}"
```
The saga becomes `FAILED`, and the order becomes `REJECTED` with reason `"<original reason> (compensation resolved manually)"`.

### Responses
| Status | Meaning |
|---|---|
| 200 | applied; the body shows from-state and to-state |
| 400 | bad saga id, unknown action, missing operator, or resolve without a note |
| 404 | unknown saga |
| 409 | the saga is no longer compensating (it may have just completed), or it changed concurrently. Re-check with GET, then retry the call if still needed |

---

## SagaCompensationSlow (ticket)
The oldest compensation has been running for more than 10 minutes but hasn't crossed the stuck threshold. Usually a participant is slow, or retrying a poison record that blocks its partition. Check the participant logs for `Delivery attempt … failed` and its DLT (step 2 above). No action is needed if it clears on its own.

## SagaCompensationMetricsMissing (ticket)
Prometheus isn't receiving `saga_compensation_*`, so stuck compensations would go unnoticed. Check that order-service is up and `/actuator/prometheus` is reachable and scraped.

---

## Dead-lettered messages (no alert yet)
Each service only sees and replays its own DLT:

| Service | DLT |
|---|---|
| order | `order.saga.replies-dlt` |
| payment | `payment.commands-dlt` |
| inventory | `inventory.commands-dlt` |

```
curl -H "Authorization: Bearer %TOKEN%" http://localhost:8082/actuator/dlt
curl -H "Authorization: Bearer %TOKEN%" -X POST http://localhost:8082/actuator/dlt/payment.commands -H "Content-Type: application/json" -d "{\"limit\":50}"
```
To look at what was dead-lettered and why (local stack), open AKHQ at http://localhost:8086 → topic `<topic>-dlt` → **Data**, and click the **Headers** count of a record. `kafka_dlt-exception-cause-fqcn` and `kafka_dlt-exception-message` give the cause. Re-send with the replay endpoint above, **not** AKHQ's produce/copy: the endpoint strips the `kafka_dlt-*` headers and tracks what has already been replayed.
- **Before replaying, fix the cause.** The `Dead-lettered … : <exception>` ERROR line names it. A record that fails again returns to the DLT with `dltReplayCount` incremented.
- **Replay is safe to repeat.** Each record keeps its `messageId`, so consumers skip anything already processed.
- **Commands for finished sagas are refused safely.** Replaying a charge or reservation for a saga that has already been compensated hits the tombstone and is refused. That's expected, not an error.
- **Replies for finished sagas are dropped** by the orchestrator's state guard (WARN `ignoring …`).
