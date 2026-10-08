package com.saga.payment.domain;

import java.util.Optional;

import jakarta.persistence.LockModeType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CustomerCreditRepository extends JpaRepository<CustomerCredit, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from CustomerCredit c where c.customerId = :customerId")
    Optional<CustomerCredit> findForUpdate(@Param("customerId") String customerId);
}
