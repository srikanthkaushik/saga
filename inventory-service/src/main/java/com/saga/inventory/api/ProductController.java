package com.saga.inventory.api;

import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
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

import com.saga.inventory.domain.Product;
import com.saga.inventory.domain.ProductRepository;

/**
 * Product stock: read, plus PUT to create or overwrite a quantity. The PUT is test/demo tooling (used by the
 * saga UI to seed scenarios) and must be locked down together with actuator.
 */
@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductRepository productRepository;

    public ProductController(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @GetMapping
    @Transactional(readOnly = true)
    public List<ProductView> list() {
        return productRepository.findAll(Sort.by("productId")).stream().map(ProductView::of).toList();
    }

    @GetMapping("/{productId}")
    @Transactional(readOnly = true)
    public ResponseEntity<ProductView> get(@PathVariable String productId) {
        return ResponseEntity.of(productRepository.findById(productId).map(ProductView::of));
    }

    @PutMapping("/{productId}")
    @Transactional
    public ProductView put(@PathVariable @Pattern(regexp = "[A-Za-z0-9._-]{1,100}") String productId,
                           @Valid @RequestBody StockRequest request) {
        Product product = productRepository.findForUpdate(productId)
                .orElseGet(() -> productRepository.save(new Product(productId, request.availableQuantity())));
        product.setAvailableQuantity(request.availableQuantity());
        return ProductView.of(product);
    }

    public record StockRequest(@Min(0) int availableQuantity) {
    }

    public record ProductView(String productId, int availableQuantity) {
        static ProductView of(Product product) {
            return new ProductView(product.getProductId(), product.getAvailableQuantity());
        }
    }
}
