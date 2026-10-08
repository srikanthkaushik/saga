create table customer_credit (
    customer_id      varchar(100) primary key,
    available_credit numeric(19, 2) not null check (available_credit >= 0)
);

create table payment (
    id          uuid primary key,
    order_id    uuid           not null unique,
    customer_id varchar(100)   not null references customer_credit (customer_id),
    amount      numeric(19, 2) not null,
    status      varchar(20)    not null,
    created_at  timestamptz    not null,
    updated_at  timestamptz    not null
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

-- Local seed data
insert into customer_credit (customer_id, available_credit) values
    ('customer-1', 1000.00),
    ('customer-2', 50.00);
