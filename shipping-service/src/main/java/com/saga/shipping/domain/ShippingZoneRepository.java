package com.saga.shipping.domain;

import java.util.Optional;

import jakarta.persistence.LockModeType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ShippingZoneRepository extends JpaRepository<ShippingZone, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select z from ShippingZone z where z.country = :country")
    Optional<ShippingZone> findForUpdate(@Param("country") String country);
}
