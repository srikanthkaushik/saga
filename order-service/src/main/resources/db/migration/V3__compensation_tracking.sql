alter table order_saga add column compensating_since timestamptz;
alter table order_saga add column compensation_resends integer not null default 0;

-- Sagas already compensating start their clock now
update order_saga set compensating_since = now() where state in ('RELEASING_INVENTORY', 'COMPENSATING');

create index ix_order_saga_compensating on order_saga (compensating_since)
    where state in ('RELEASING_INVENTORY', 'COMPENSATING');
