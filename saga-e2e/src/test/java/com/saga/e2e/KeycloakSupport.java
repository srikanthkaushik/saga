package com.saga.e2e;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Keycloak for the e2e suite: the same image and realm file as docker-compose, plus helpers to obtain tokens
 * (password grant via the dev-only {@code saga-cli} client, client credentials) and to adjust the realm.
 */
final class KeycloakSupport {

    static final String IMAGE = "quay.io/keycloak/keycloak:26.7.0";
    private static final Duration TOKEN_REUSE = Duration.ofSeconds(60);

    private final GenericContainer<?> container;
    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    KeycloakSupport(Path realmFile) {
        this.container = new GenericContainer<>(IMAGE)
                .withExposedPorts(8080)
                .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
                .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
                .withCopyFileToContainer(MountableFile.forHostPath(realmFile), "/opt/keycloak/data/import/saga-realm.json")
                .withCommand("start-dev", "--import-realm")
                .waitingFor(Wait.forHttp("/realms/saga/.well-known/openid-configuration")
                        .forPort(8080)
                        .withStartupTimeout(Duration.ofMinutes(3)));
    }

    GenericContainer<?> container() {
        return container;
    }

    /** Must be the exact URL every party uses, because it ends up as the tokens' {@code iss} claim. */
    String baseUrl() {
        return "http://" + container.getHost() + ":" + container.getMappedPort(8080);
    }

    String issuer() {
        return baseUrl() + "/realms/saga";
    }

    /** Access token for a dev user (viewer, operator, admin; password = username), reused for up to a minute. */
    String token(String user) {
        return cached("user:" + user, () -> tokenRequest("saga", Map.of(
                "grant_type", "password", "client_id", "saga-cli", "username", user, "password", user)));
    }

    String clientCredentialsToken(String clientId, String secret) {
        return cached("client:" + clientId, () -> tokenRequest("saga", Map.of(
                "grant_type", "client_credentials", "client_id", clientId, "client_secret", secret)));
    }

    /** A perfectly valid token - from the wrong realm (master), so wrong issuer and no saga-api audience. */
    String foreignRealmToken() {
        return tokenRequest("master", Map.of(
                "grant_type", "password", "client_id", "admin-cli", "username", "admin", "password", "admin"));
    }

    /** Points the console client's redirect URIs at the console's actual (random) port. */
    void allowConsoleAt(String consoleBaseUrl) throws Exception {
        String admin = foreignRealmToken();
        HttpResponse<String> found = http.send(HttpRequest.newBuilder(
                        URI.create(baseUrl() + "/admin/realms/saga/clients?clientId=saga-console"))
                .header("Authorization", "Bearer " + admin).build(), HttpResponse.BodyHandlers.ofString());
        ObjectNode client = (ObjectNode) json.readTree(found.body()).get(0);
        client.putArray("redirectUris").add(consoleBaseUrl + "/*");
        ((ObjectNode) client.get("attributes")).put("post.logout.redirect.uris", consoleBaseUrl + "/*");
        HttpResponse<String> updated = http.send(HttpRequest.newBuilder(
                        URI.create(baseUrl() + "/admin/realms/saga/clients/" + client.get("id").asString()))
                .header("Authorization", "Bearer " + admin)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(client))).build(),
                HttpResponse.BodyHandlers.ofString());
        if (updated.statusCode() != 204) {
            throw new IllegalStateException("Updating saga-console redirect URIs failed: " + updated.statusCode() + " " + updated.body());
        }
    }

    private String tokenRequest(String realm, Map<String, String> form) {
        String body = form.entrySet().stream()
                .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
        try {
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                            URI.create(baseUrl() + "/realms/" + realm + "/protocol/openid-connect/token"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
            JsonNode token = json.readTree(response.body()).get("access_token");
            if (token == null) {
                throw new IllegalStateException("No token from realm " + realm + ": " + response.body());
            }
            return token.asString();
        } catch (Exception e) {
            throw new IllegalStateException("Token request failed", e);
        }
    }

    private String cached(String key, java.util.function.Supplier<String> fetch) {
        Cached hit = cache.get(key);
        if (hit != null && hit.until().isAfter(Instant.now())) {
            return hit.token();
        }
        String token = fetch.get();
        cache.put(key, new Cached(token, Instant.now().plus(TOKEN_REUSE)));
        return token;
    }

    private record Cached(String token, Instant until) {
    }

    static List<String> users() {
        return List.of("viewer", "operator", "admin");
    }
}
