package com.saga.shipping.api;

import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.saga.shipping.domain.ShippingZone;
import com.saga.shipping.domain.ShippingZoneRepository;

/**
 * Destinations a carrier serves: read, plus PUT to create a zone or open/close one. The PUT is test/demo tooling
 * (the console uses it to make shipping fail for a country) and is admin-only.
 */
@RestController
@RequestMapping("/zones")
public class ZoneController {

    private final ShippingZoneRepository zoneRepository;

    public ZoneController(ShippingZoneRepository zoneRepository) {
        this.zoneRepository = zoneRepository;
    }

    @GetMapping
    @Transactional(readOnly = true)
    public List<ZoneView> list() {
        return zoneRepository.findAll(Sort.by("country")).stream().map(ZoneView::of).toList();
    }

    @GetMapping("/{country}")
    @Transactional(readOnly = true)
    public ResponseEntity<ZoneView> get(@PathVariable String country) {
        return ResponseEntity.of(zoneRepository.findById(country).map(ZoneView::of));
    }

    @PutMapping("/{country}")
    @Transactional
    public ZoneView put(@PathVariable @Pattern(regexp = "[A-Z]{2}") String country,
                        @Valid @RequestBody ZoneRequest request) {
        ShippingZone zone = zoneRepository.findForUpdate(country)
                .orElseGet(() -> zoneRepository.save(new ShippingZone(country, country, request.served())));
        zone.setServed(request.served());
        if (request.name() != null && !request.name().isBlank()) {
            zone.setName(request.name());
        }
        return ZoneView.of(zone);
    }

    /** {@code name} is optional; omitted, an existing zone keeps its name and a new one is named after its code. */
    public record ZoneRequest(boolean served, @Size(max = 100) String name) {
    }

    public record ZoneView(String country, String name, boolean served) {
        static ZoneView of(ShippingZone zone) {
            return new ZoneView(zone.getCountry(), zone.getName(), zone.isServed());
        }
    }
}
