# Keycloak with the 'saga' realm baked in, so a deployment needs no files besides docker-compose.yml and .env.
# The realm uses ${VAR:default} placeholders (console URL, client secrets, dev-user passwords) that the
# deployment sets through environment variables; unset ones fall back to the local-development values.
# Pinned: the ${VAR:default} form is verified on this Keycloak version.
FROM quay.io/keycloak/keycloak:26.7.0
LABEL org.opencontainers.image.source="https://github.com/srikanthkaushik/saga"
COPY docker/keycloak/saga-realm.json /opt/keycloak/data/import/saga-realm.json
CMD ["start-dev", "--import-realm"]
