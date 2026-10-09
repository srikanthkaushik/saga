package com.saga.payment.api;

import java.math.BigDecimal;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.saga.payment.domain.CustomerCredit;
import com.saga.payment.domain.CustomerCreditRepository;

/**
 * Customer credit: read, plus PUT to create or overwrite a balance. The PUT is test/demo tooling (used by the
 * saga UI to seed scenarios) and must be locked down together with actuator.
 */
@RestController
@RequestMapping("/customers")
public class CustomerController {

    private final CustomerCreditRepository creditRepository;

    public CustomerController(CustomerCreditRepository creditRepository) {
        this.creditRepository = creditRepository;
    }

    @GetMapping
    @Transactional(readOnly = true)
    public List<CustomerView> list() {
        return creditRepository.findAll(Sort.by("customerId")).stream().map(CustomerView::of).toList();
    }

    @GetMapping("/{customerId}")
    @Transactional(readOnly = true)
    public ResponseEntity<CustomerView> get(@PathVariable String customerId) {
        return ResponseEntity.of(creditRepository.findById(customerId).map(CustomerView::of));
    }

    @PutMapping("/{customerId}")
    @Transactional
    public CustomerView put(@PathVariable @Pattern(regexp = "[A-Za-z0-9._-]{1,100}") String customerId,
                            @Valid @RequestBody CreditRequest request) {
        CustomerCredit credit = creditRepository.findForUpdate(customerId)
                .orElseGet(() -> creditRepository.save(new CustomerCredit(customerId, request.availableCredit())));
        credit.setAvailableCredit(request.availableCredit());
        return CustomerView.of(credit);
    }

    public record CreditRequest(@NotNull @DecimalMin("0.00") @Digits(integer = 17, fraction = 2) BigDecimal availableCredit) {
    }

    public record CustomerView(String customerId, BigDecimal availableCredit) {
        static CustomerView of(CustomerCredit credit) {
            return new CustomerView(credit.getCustomerId(), credit.getAvailableCredit());
        }
    }
}
