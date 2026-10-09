package com.saga.e2e;

import java.io.IOException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.HttpCookie;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A scripted browser for the saga console: follows the OIDC redirect to Keycloak, submits the login form, and
 * keeps the session cookie. POSTs can be sent with or without the CSRF header to check both paths.
 */
final class ConsoleSession {

    private static final Pattern LOGIN_FORM_ACTION = Pattern.compile("<form[^>]*id=\"kc-form-login\"[^>]*action=\"([^\"]+)\"");

    private final String baseUrl;
    private final CookieManager cookies = new LocalhostCookies();
    private final HttpClient http;

    private ConsoleSession(String baseUrl) {
        this.baseUrl = baseUrl;
        this.http = HttpClient.newBuilder().cookieHandler(cookies).followRedirects(HttpClient.Redirect.NORMAL).build();
    }

    static ConsoleSession login(String consoleBaseUrl, String user, String password) throws Exception {
        ConsoleSession session = new ConsoleSession(consoleBaseUrl);
        // 1. The console redirects a browser to Keycloak's login page
        HttpResponse<String> loginPage = session.http.send(HttpRequest.newBuilder(URI.create(consoleBaseUrl + "/"))
                .header("Accept", "text/html").build(), HttpResponse.BodyHandlers.ofString());
        Matcher action = LOGIN_FORM_ACTION.matcher(loginPage.body());
        if (!action.find()) {
            throw new IllegalStateException("No Keycloak login form at " + loginPage.uri() + " (HTTP " + loginPage.statusCode() + ")");
        }
        // 2. Submit credentials; Keycloak redirects back to the console's callback, which redirects to /
        String form = "username=" + URLEncoder.encode(user, StandardCharsets.UTF_8)
                + "&password=" + URLEncoder.encode(password, StandardCharsets.UTF_8) + "&credentialId=";
        HttpResponse<String> landed = session.http.send(HttpRequest.newBuilder(URI.create(action.group(1).replace("&amp;", "&")))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "text/html")
                .POST(HttpRequest.BodyPublishers.ofString(form)).build(), HttpResponse.BodyHandlers.ofString());
        if (!landed.uri().toString().startsWith(consoleBaseUrl) || landed.statusCode() != 200) {
            throw new IllegalStateException("Login as " + user + " did not land on the console: " + landed.uri() + " HTTP " + landed.statusCode());
        }
        return session;
    }

    HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).build(), HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> send(String method, String path, String body, boolean withCsrf) throws Exception {
        get("/api/me"); // make sure the XSRF-TOKEN cookie has been issued
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body));
        if (withCsrf) {
            request.header("X-XSRF-TOKEN", csrfToken());
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Keycloak marks its login cookies {@code Secure; SameSite=None}. Browsers still send those to http://localhost
     * (a "potentially trustworthy" origin); java.net's CookieManager never does over plain HTTP, so the login form
     * POST would arrive without its session cookie. Strip {@code Secure} to behave like a browser on localhost.
     */
    private static final class LocalhostCookies extends CookieManager {

        LocalhostCookies() {
            super(null, CookiePolicy.ACCEPT_ALL);
        }

        @Override
        public void put(URI uri, Map<String, List<String>> responseHeaders) throws IOException {
            Map<String, List<String>> headers = new HashMap<>();
            responseHeaders.forEach((name, values) -> headers.put(name,
                    name != null && name.toLowerCase(Locale.ROOT).startsWith("set-cookie")
                            ? values.stream().map(value -> value.replaceAll("(?i);\\s*Secure", "")).toList()
                            : values));
            super.put(uri, headers);
        }
    }

    private String csrfToken() {
        return cookies.getCookieStore().getCookies().stream()
                .filter(cookie -> cookie.getName().equals("XSRF-TOKEN"))
                .map(HttpCookie::getValue)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No XSRF-TOKEN cookie"));
    }
}
