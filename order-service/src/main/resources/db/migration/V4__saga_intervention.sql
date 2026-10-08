-- Audit trail of operator actions on sagas (retry / resolve)
create table saga_intervention (
    id         uuid primary key,
    saga_id    uuid         not null references order_saga (id),
    action     varchar(20)  not null,
    operator   varchar(200) not null,
    note       varchar(2000),
    from_state varchar(30)  not null,
    to_state   varchar(30)  not null,
    created_at timestamptz  not null
);

create index ix_saga_intervention_saga on saga_intervention (saga_id, created_at);
