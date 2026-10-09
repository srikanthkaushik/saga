package com.saga.ui;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/**
 * Same-origin pass-through: {@code /api/order/orders?limit=5} -> {@code <order-service>/orders?limit=5}.
 * Method, query string, body, Content-Type and Accept are forwarded; status, Content-Type and body come back
 * untouched (4xx/5xx included), so the UI sees exactly what the service answered. An unreachable service is 502.
 */
@RestController
public class ProxyController {

    /** DLT replay can legitimately take up to 30s. */
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(35);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);

    private final Map<String, URI> targets;
    private final RestClient restClient;

    public ProxyController(UiProperties properties) {
        this.targets = properties.targets();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
        requestFactory.setReadTimeout(READ_TIMEOUT);
        this.restClient = RestClient.builder().requestFactory(requestFactory).build();
    }

    @RequestMapping("/api/{service:order|payment|inventory}/**")
    public ResponseEntity<byte[]> proxy(@PathVariable String service,
                                        HttpServletRequest request,
                                        @RequestBody(required = false) byte[] body) {
        URI target = targetUri(service, request);
        RestClient.RequestBodySpec spec = restClient.method(HttpMethod.valueOf(request.getMethod()))
                .uri(target)
                .headers(headers -> {
                    copyHeader(request, headers, HttpHeaders.CONTENT_TYPE);
                    copyHeader(request, headers, HttpHeaders.ACCEPT);
                });
        if (body != null && body.length > 0) {
            spec.body(body);
        }
        try {
            return spec.exchange((clientRequest, response) -> {
                ResponseEntity.BodyBuilder builder = ResponseEntity.status(response.getStatusCode());
                MediaType contentType = response.getHeaders().getContentType();
                if (contentType != null) {
                    builder.contentType(contentType);
                }
                return builder.body(response.getBody().readAllBytes());
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
