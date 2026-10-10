# Postgres with the script that creates the service databases (order_db, payment_db, inventory_db, shipping_db, notification_db) on first start.
FROM postgres:17
LABEL org.opencontainers.image.source="https://github.com/srikanthkaushik/saga"
COPY docker/postgres/init.sql /docker-entrypoint-initdb.d/init.sql
