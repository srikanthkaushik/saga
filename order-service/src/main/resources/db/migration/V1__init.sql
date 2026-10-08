create table orders (
    id               uuid primary key,
    customer_id      varchar(100)   not null,
    product_id       varchar(100)   not null,
    quantity         integer        not null check (quantity > 0),
    amount           numeric(19, 2) not null check (amount > 0),
    status           varchar(20)    not null,
    rejection_reason varchar(500),
    version          bigint         not null,
    created_at       timestamptz    not null,
    updated_at       timestamptz    not null
);

create table order_saga (
    id             uuid primary key,
    order_id       uuid         not null unique references orders (id),
    state          varchar(30)  not null,
    failure_reason varchar(500),
    version        bigint       not null,
    created_at     timestamptz  not null,
    updated_at     timestamptz  not null
);

create table outbox (
    id           uuid primary key,
    topic        varchar(255) not null,
    message_key  varchar(255) not null,
    message_type varchar(100) not null,
    payload      text         not null,
    created_at   timestamptz  not null,
    published_at timestamptz
);

create index ix_outbox_unpublished on outbox (created_at) where published_at is null;

create table processed_message (
    message_id   uuid primary key,
    processed_at timestamptz not null
);
