-- CANCELLED tombstones record "refunded before charged" and carry no customer or amount
alter table payment alter column customer_id drop not null;
alter table payment alter column amount drop not null;
alter table payment add constraint ck_payment_charged_fields
    check (status = 'CANCELLED' or (customer_id is not null and amount is not null));
