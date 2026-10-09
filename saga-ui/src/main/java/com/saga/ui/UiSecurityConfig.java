package com.saga.ui;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.authority.mapping.GrantedAuthoritiesMapper;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientProviderBuilder;
import org.springframework.security.oauth2.client.oidc.web.logout.OidcClientInitiatedLogoutSuccessHandler;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.DefaultOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizedClientRepository;
import org.springframework.security.oauth2.core.oidc.user.OidcUserAuthority;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher;

/**
 * The console is an OIDC client of Keycloak: every page needs a login, the user's access token stays in the
 * server-side session and is relayed by {@link ProxyController}. Browser-facing requests carry only the session
 * cookie, so CSRF protection is on ({@code csrf.spa()}: cookie {@code XSRF-TOKEN}, header {@code X-XSRF-TOKEN}).
 */
@Configuration
public class UiSecurityConfig {

    static final String OPERATOR = "saga-operator";
    /** Lets the page tell "console session expired" apart from a 401 relayed from a service. */
    static final String LOGIN_REQUIRED_HEADER = "X-Saga-Console-Login";

    @Bean
    SecurityFilterChain uiSecurityFilterChain(HttpSecurity http, ClientRegistrationRepository registrations)
            throws Exception {
        OidcClientInitiatedLogoutSuccessHandler keycloakLogout = new OidcClientInitiatedLogoutSuccessHandler(registrations);
        keycloakLogout.setPostLogoutRedirectUri("{baseUrl}/");

        http
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health", "/actuator/health/**", "/error").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/inject").hasRole(OPERATOR)
                        .anyRequest().authenticated())
                .oauth2Login(Customizer.withDefaults())
                // A plain link signs out (ends the Keycloak session too). GET only risks a forced logout; acceptable here.
                .logout(logout -> logout
                        .logoutRequestMatcher(PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.GET, "/logout"))
                        .logoutSuccessHandler(keycloakLogout))
                .csrf(csrf -> csrf.spa())
                // API calls get a 401 instead of a redirect to the login page, so the page can reload into the login
                .exceptionHandling(errors -> errors.defaultAuthenticationEntryPointFor(
                        (request, response, exception) -> {
                            response.setHeader(LOGIN_REQUIRED_HEADER, "true");
                            response.sendError(HttpStatus.UNAUTHORIZED.value());
                        },
                        PathPatternRequestMatcher.withDefaults().matcher("/api/**")));
        return http.build();
    }

    /** Keycloak puts the user's realm roles in the ID token's {@code roles} claim; expose them as {@code ROLE_*}. */
    @Bean
    GrantedAuthoritiesMapper keycloakRolesMapper() {
        return authorities -> {
            Set<GrantedAuthority> mapped = new HashSet<>(authorities);
            for (GrantedAuthority authority : authorities) {
                if (authority instanceof OidcUserAuthority oidc
                        && oidc.getIdToken().getClaims().get("roles") instanceof Collection<?> roles) {
                    roles.forEach(role -> mapped.add(new SimpleGrantedAuthority("ROLE_" + role)));
                }
            }
            return mapped;
        };
    }

    /** Returns the signed-in user's access token, refreshing it with the refresh token when it has expired. */
    @Bean
    OAuth2AuthorizedClientManager authorizedClientManager(ClientRegistrationRepository registrations,
                                                          OAuth2AuthorizedClientRepository authorizedClients) {
        DefaultOAuth2AuthorizedClientManager manager =
                new DefaultOAuth2AuthorizedClientManager(registrations, authorizedClients);
        manager.setAuthorizedClientProvider(OAuth2AuthorizedClientProviderBuilder.builder()
                .authorizationCode()
                .refreshToken()
                .build());
        return manager;
    }
}
