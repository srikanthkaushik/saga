-- Countries a carrier delivers to. An unserved (or unknown) destination makes ScheduleShipment fail.
create table shipping_zone (
    country varchar(2)   primary key,
    name    varchar(100) not null,
    served  boolean      not null
);

create table shipment (
    id              uuid primary key,
    order_id        uuid         not null unique,
    product_id      varchar(100) not null,
    quantity        integer      not null check (quantity > 0),
    destination     varchar(2)   not null,
    status          varchar(20)  not null,
    tracking_number varchar(40),
    created_at      timestamptz  not null,
    cancelled_at    timestamptz
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
create index ix_outbox_message_key on outbox (message_key);

create table processed_message (
    message_id   uuid primary key,
    processed_at timestamptz not null
);

-- Local seed data: XX is a deliberately unserved destination for demonstrating a failed shipping step
insert into shipping_zone (country, name, served) values
    ('GB', 'United Kingdom', true),
    ('US', 'United States', true),
    ('DE', 'Germany', true),
    ('XX', 'Nowhere (no carrier)', false);
