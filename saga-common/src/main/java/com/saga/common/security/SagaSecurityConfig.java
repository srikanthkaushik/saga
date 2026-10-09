package com.saga.common.security;

import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * The single access rulebook for order-, payment- and inventory-service: OAuth2 resource server validating
 * Keycloak JWTs (issuer, signature, audience {@code saga-api}). Roles come from the token's {@code roles} claim,
 * mapped to {@code ROLE_*} by the {@code spring.security.oauth2.resourceserver.jwt.*} properties in each service.
 * <p>
 * Rules are evaluated top to bottom; anything not listed is denied. Paths that a given service doesn't have
 * (e.g. {@code /customers} on order-service) simply never match.
 * <pre>
 * public          GET /actuator/health[/**], POST /orders, GET /orders/{id}, /error
 * metrics|viewer  GET /actuator/prometheus
 * viewer          GET /actuator/**, GET /orders (list), GET /customers|products|payments|reservations/**
 * operator        POST /actuator/dlt/**, POST /actuator/stucksagas/**
 * admin           PUT /customers/**, PUT /products/**
 * </pre>
 * Roles are composite in Keycloak (admin ⊃ operator ⊃ viewer), so higher roles pass lower checks.
 */
@Configuration
public class SagaSecurityConfig {

    public static final String VIEWER = "saga-viewer";
    public static final String OPERATOR = "saga-operator";
    public static final String ADMIN = "saga-admin";
    public static final String METRICS = "saga-metrics";

    @Bean
    SecurityFilterChain sagaSecurityFilterChain(HttpSecurity http) throws Exception {
        http
                // Bearer tokens only: no cookies, no sessions, so no CSRF exposure
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(GET, "/actuator/health", "/actuator/health/**").permitAll()
                        .requestMatchers(POST, "/orders").permitAll()
                        .requestMatchers(GET, "/orders/*").permitAll()
                        .requestMatchers("/error").permitAll()
                        .requestMatchers(GET, "/actuator/prometheus").hasAnyRole(METRICS, VIEWER)
                        .requestMatchers(GET, "/actuator/**", "/orders",
                                "/customers/**", "/products/**", "/payments/**", "/reservations/**").hasRole(VIEWER)
                        .requestMatchers(POST, "/actuator/dlt/**", "/actuator/stucksagas/**").hasRole(OPERATOR)
                        .requestMatchers(PUT, "/customers/**", "/products/**").hasRole(ADMIN)
                        .anyRequest().denyAll())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()));
        return http.build();
    }
}
