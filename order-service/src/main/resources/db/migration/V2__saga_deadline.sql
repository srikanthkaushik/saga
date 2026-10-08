alter table order_saga add column deadline timestamptz;

-- Sagas already in flight get a fresh deadline rather than timing out the moment this deploys
update order_saga set deadline = now() + interval '1 minute' where state not in ('COMPLETED', 'FAILED');

create index ix_order_saga_deadline on order_saga (deadline) where deadline is not null;
