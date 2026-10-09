# Deploying the saga stack on Unraid

The whole development stack runs on an Unraid server as one Docker Compose project: Postgres, Kafka, AKHQ, Keycloak, the three services and the console. It uses the **Docker Compose Manager** plugin, images from GitHub Container Registry (GHCR), and plain HTTP on your LAN.

```
Windows PC (this repo)                       Unraid server (SAGA_HOST)
  ops\images.cmd <tag> push  ──► ghcr.io ──►  docker compose pull / up
                                              ├─ saga-postgres        :5432   data: ${APPDATA}/postgres
                                              ├─ saga-kafka           :9092   data: ${APPDATA}/kafka
                                              ├─ saga-akhq            :8086
                                              ├─ saga-keycloak        :8180   realm baked into the image
                                              ├─ saga-order-service   :8081
                                              ├─ saga-payment-service :8082
                                              ├─ saga-inventory-service :8083
                                              └─ saga-console         :8080   ← start here in the browser
```

## 1. Build and push the images (Windows, CMD)

**One-time setup:**
1. On GitHub: **Settings → Developer settings → Personal access tokens → Tokens (classic)**. Create a token with the `write:packages` scope; GitHub adds `read:packages` automatically.
2. Log in:
   ```
   docker login ghcr.io -u srikanthkaushik
   ```
   Paste the token as the password.

**Each release** (from the repository root):
```
ops\images.cmd 2026.10.09 push
```
This packages the jars, builds six images (`saga-order-service`, `saga-payment-service`, `saga-inventory-service`, `saga-console`, `saga-keycloak`, `saga-postgres`), tags each with the given tag and `latest`, and pushes both tags.
- Without `push`, it only builds.
- To use another registry, set `SAGA_REGISTRY` first.

**Make the images reachable from Unraid.** GHCR packages are **private** by default. Choose one:
- Make the six packages public (**GitHub → your profile → Packages → package → Package settings → Change visibility**), or
- Keep them private and log in once on Unraid (step 2.3) with a token that has `read:packages`.

## 2. Set up the project on Unraid

1. **Install the plugin.** In **Apps** (Community Applications), install **Docker Compose Manager**.
2. **Create the stack.** In **Docker**, under *Compose*, click **Add New Stack** and name it `saga`. Then:
   - **Edit Stack → Compose File:** paste [`docker-compose.yml`](docker-compose.yml).
   - **Edit Stack → Env File:** paste [`.env.example`](.env.example), then adjust it:

   | Setting | Set it to |
   |---|---|
   | `SAGA_HOST` | the server's LAN IP (e.g. `192.168.1.50`), or a hostname **every** device and container resolves. Browsers and the containers both use it to reach Keycloak |
   | `*_PORT` | change any port another container already uses. Port 8080 is a common clash; e.g. `CONSOLE_PORT=8088` |
   | `APPDATA` | where Postgres and Kafka keep data. A cache-pool path such as `/mnt/cache/appdata/saga` is best for databases |
   | all `change-me-*` | your own passwords and secrets |
   | `TZ` | your time zone |

3. **Private images only:** open the Unraid terminal once and run
   ```
   docker login ghcr.io -u srikanthkaushik
   ```
   with a token that has `read:packages`.
4. **Start it:** **Compose Up**. The first start pulls the images and creates the data folders. It takes about a minute until everything is healthy:
   - Postgres initialises the three databases;
   - Keycloak imports the realm;
   - the services run their migrations and create the Kafka topics.

   The start order is enforced by healthchecks: Postgres, Kafka and Keycloak first, then the services, then the console.

## 3. Use it

| What | URL |
|---|---|
| Saga console | `http://SAGA_HOST:CONSOLE_PORT`. Sign in as `admin` with `SAGA_ADMIN_PASSWORD` |
| Keycloak admin console | `http://SAGA_HOST:KEYCLOAK_PORT`. User `admin`, password `KEYCLOAK_ADMIN_PASSWORD` |
| AKHQ (Kafka UI) | `http://SAGA_HOST:AKHQ_PORT` |
| Services (curl, API console) | `http://SAGA_HOST:8081` / `8082` / `8083` |

The users are `viewer`, `operator` and `admin`, with the passwords from `.env`. Roles are as in [Getting started §4](../../docs/GETTING-STARTED.md#4-sign-in-users-roles-and-tokens).

**curl from your PC:** point the token helper at the server, then call the services:
```
set SAGA_KEYCLOAK_URL=http://SAGA_HOST:8180
call ops\token.cmd admin <SAGA_ADMIN_PASSWORD>
curl -H "Authorization: Bearer %TOKEN%" http://SAGA_HOST:8081/actuator/stucksagas
```

Everything in [Scenarios](../../docs/SCENARIOS.md) works the same way. Use `SAGA_HOST` instead of `localhost`, and run `docker exec` commands in the Unraid terminal. The container names are the same: `saga-postgres`, `saga-kafka`, …

## 4. Update

**Push new images:** run `ops\images.cmd <new-tag> push` on Windows. Then on Unraid, either:
- keep `SAGA_TAG=latest` and use **Update Stack**, or **Compose Pull** followed by **Compose Up**; or
- set `SAGA_TAG=<new-tag>` and **Compose Up**.

What survives an update:
- **Postgres and Kafka data** survive in `APPDATA`.
- **Keycloak** keeps no data of its own (development mode). It re-imports the realm when its container is **re-created**, so changes to the saga user passwords or client secrets in `.env` take effect then. A simple restart keeps the old values.
- **`POSTGRES_USER` / `POSTGRES_PASSWORD`** are applied only when the Postgres data folder is first created. To change them later, change them inside Postgres too (or wipe, see section 5).

## 5. Reset

**Compose Down**, delete the `APPDATA` folder, then **Compose Up**. This removes all orders, Kafka topics and offsets. Delete Postgres and Kafka data **together**: if you delete only one, the other refers to sagas that no longer exist.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| A service or the console keeps restarting; the logs say `Connection refused` for `http://SAGA_HOST:8180/...` | Containers can't reach the server at `SAGA_HOST`. Use the server's LAN IP; a hostname must resolve inside containers too. The stack needs the default bridge networking it creates, so don't put these containers on `br0`/macvlan (custom IP), because macvlan containers can't reach their own host |
| Login fails with `Invalid parameter: redirect_uri` | The console's address in the browser doesn't match `http://SAGA_HOST:CONSOLE_PORT`. Use exactly that URL (same host form, IP vs name) |
| `401` from the services after login | Tokens are only valid for the issuer `http://SAGA_HOST:KEYCLOAK_PORT/realms/saga`. Every client must reach Keycloak by exactly that address |
| `pull access denied` / `unauthorized` when pulling | The images are private: make them public or run `docker login ghcr.io` on Unraid (step 2.3) |
| Kafka exits with `Invalid cluster.id` | `KAFKA_CLUSTER_ID` changed while `APPDATA/kafka` has data. Restore the old value, or delete `APPDATA/kafka` together with `APPDATA/postgres` |
| Port already in use | Change the `*_PORT` setting in `.env`. If you change `CONSOLE_PORT` or `KEYCLOAK_PORT`, re-create Keycloak so its realm picks up the new console URL |
