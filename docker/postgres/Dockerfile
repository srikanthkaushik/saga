# Postgres with the script that creates order_db, payment_db and inventory_db on first start.
FROM postgres:17
LABEL org.opencontainers.image.source="https://github.com/srikanthkaushik/saga"
COPY docker/postgres/init.sql /docker-entrypoint-initdb.d/init.sql
