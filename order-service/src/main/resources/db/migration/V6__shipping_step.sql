-- Shipping becomes the third saga step: orders carry a destination country
alter table orders add column destination varchar(2) not null default 'GB';

-- CANCELLING_SHIPMENT is a compensating state too: recreate the partial index the stuck-saga queries use
drop index ix_order_saga_compensating;
create index ix_order_saga_compensating on order_saga (compensating_since)
    where state in ('CANCELLING_SHIPMENT', 'RELEASING_INVENTORY', 'COMPENSATING');
