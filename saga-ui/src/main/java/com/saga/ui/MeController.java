package com.saga.ui;

import java.util.List;
import java.util.Map;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /api/me} - who is signed in to the console and with which saga roles, for the page header and role-gated buttons. */
@RestController
public class MeController {

    @GetMapping("/api/me")
    public Map<String, Object> me(OAuth2AuthenticationToken authentication) {
        OidcUser user = (OidcUser) authentication.getPrincipal();
        // The mapped ROLE_* authorities live on the authentication; the OidcUser principal keeps the unmapped ones
        List<String> roles = authentication.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .filter(authority -> authority.startsWith("ROLE_saga-"))
                .map(authority -> authority.substring("ROLE_".length()))
                .sorted()
                .toList();
        return Map.of(
                "username", user.getPreferredUsername(),
                "name", user.getFullName() == null ? user.getPreferredUsername() : user.getFullName(),
                "roles", roles);
    }
}
