package com.saga.ui;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.OAuth2AuthorizeRequest;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/**
 * Same-origin pass-through: {@code /api/order/orders?limit=5} -> {@code <order-service>/orders?limit=5}.
 * Method, query string, body, Content-Type and Accept are forwarded, plus the signed-in user's Keycloak access
 * token as {@code Authorization: Bearer} (token relay; the browser never sees the token). Status, Content-Type and
 * body come back untouched (4xx/5xx included), so the UI sees exactly what the service answered - including 403
 * when the user's roles don't allow an action. An unreachable service is 502.
 */
@RestController
public class ProxyController {

    /** DLT replay can legitimately take up to 30s. */
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(35);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);

    private static final String REGISTRATION_ID = "keycloak";

    private final Map<String, URI> targets;
    private final RestClient restClient;
    private final OAuth2AuthorizedClientManager authorizedClients;

    public ProxyController(UiProperties properties, OAuth2AuthorizedClientManager authorizedClients) {
        this.targets = properties.targets();
        this.authorizedClients = authorizedClients;
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
        requestFactory.setReadTimeout(READ_TIMEOUT);
        this.restClient = RestClient.builder().requestFactory(requestFactory).build();
    }

    @RequestMapping("/api/{service:order|payment|inventory}/**")
    public ResponseEntity<byte[]> proxy(@PathVariable String service,
                                        HttpServletRequest request,
                                        HttpServletResponse response,
                                        Authentication authentication,
                                        @RequestBody(required = false) byte[] body) {
        OAuth2AuthorizedClient authorized = authorizedClients.authorize(
                OAuth2AuthorizeRequest.withClientRegistrationId(REGISTRATION_ID)
                        .principal(authentication)
                        .attribute(HttpServletRequest.class.getName(), request)
                        .attribute(HttpServletResponse.class.getName(), response)
                        .build());
        if (authorized == null) {
            // Refresh token expired or revoked: make the page sign in again
            return ResponseEntity.status(401).header(UiSecurityConfig.LOGIN_REQUIRED_HEADER, "true").build();
        }

        URI target = targetUri(service, request);
        RestClient.RequestBodySpec spec = restClient.method(HttpMethod.valueOf(request.getMethod()))
                .uri(target)
                .headers(headers -> {
                    copyHeader(request, headers, HttpHeaders.CONTENT_TYPE);
                    copyHeader(request, headers, HttpHeaders.ACCEPT);
                    headers.setBearerAuth(authorized.getAccessToken().getTokenValue());
                });
        if (body != null && body.length > 0) {
            spec.body(body);
        }
        try {
            return spec.exchange((clientRequest, upstream) -> {
                ResponseEntity.BodyBuilder builder = ResponseEntity.status(upstream.getStatusCode());
                MediaType contentType = upstream.getHeaders().getContentType();
                if (contentType != null) {
                    builder.contentType(contentType);
                }
                return builder.body(upstream.getBody().readAllBytes());
            });
        } catch (ResourceAccessException e) {
            String json = "{\"error\":\"" + service + "-service unreachable at " + targets.get(service) + "\"}";
            return ResponseEntity.status(502).contentType(MediaType.APPLICATION_JSON)
                    .body(json.getBytes(StandardCharsets.UTF_8));
        }
    }

    private URI targetUri(String service, HttpServletRequest request) {
        String prefix = request.getContextPath() + "/api/" + service;
        String path = request.getRequestURI().substring(prefix.length());   // still percent-encoded
        String query = request.getQueryString();
        return URI.create(targets.get(service) + (path.isEmpty() ? "/" : path) + (query == null ? "" : "?" + query));
    }

    private static void copyHeader(HttpServletRequest request, HttpHeaders headers, String name) {
        String value = request.getHeader(name);
        if (value != null) {
            headers.set(name, value);
        }
    }
}
