-- One row per notification actually handed to the provider. (order_id, type) is unique: a customer is told about an
-- order's outcome once, however often the event is delivered or replayed.
create table notification (
    id          uuid primary key,
    order_id    uuid          not null,
    type        varchar(30)   not null,
    customer_id varchar(100)  not null,
    channel     varchar(20)   not null,
    recipient   varchar(200)  not null,
    subject     varchar(200)  not null,
    body        varchar(2000) not null,
    sent_at     timestamptz   not null,
    unique (order_id, type)
);

create index ix_notification_sent_at on notification (sent_at);

-- notification-service emits nothing, but the shared outbox relay and its entity expect the table
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
