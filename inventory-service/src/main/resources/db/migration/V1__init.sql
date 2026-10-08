create table product (
    product_id         varchar(100) primary key,
    available_quantity integer not null check (available_quantity >= 0)
);

create table reservation (
    id         uuid primary key,
    order_id   uuid         not null unique,
    product_id varchar(100) not null references product (product_id),
    quantity   integer      not null check (quantity > 0),
    created_at timestamptz  not null
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
insert into product (product_id, available_quantity) values
    ('product-1', 100),
    ('product-2', 0);
