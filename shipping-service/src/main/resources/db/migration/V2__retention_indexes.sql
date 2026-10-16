-- Used by the retention cleanup (RetentionJob): range-delete old published outbox rows and old idempotency markers
create index ix_outbox_published_at on outbox (published_at);
create index ix_processed_message_processed_at on processed_message (processed_at);
