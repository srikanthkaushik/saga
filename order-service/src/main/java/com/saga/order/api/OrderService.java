package com.saga.order.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.saga.common.messaging.SagaCommand.ProcessPayment;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxWriter;
import com.saga.order.domain.Order;
import com.saga.order.domain.OrderRepository;
import com.saga.order.saga.OrderSaga;
import com.saga.order.saga.OrderSagaRepository;
import com.saga.order.saga.SagaTimeoutProperties;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderSagaRepository sagaRepository;
    private final OutboxWriter outbox;
    private final SagaTimeoutProperties timeouts;

    public OrderService(OrderRepository orderRepository,
                        OrderSagaRepository sagaRepository,
                        OutboxWriter outbox,
                        SagaTimeoutProperties timeouts) {
        this.orderRepository = orderRepository;
        this.sagaRepository = sagaRepository;
        this.outbox = outbox;
        this.timeouts = timeouts;
    }

    @Transactional
    public OrderResponse create(CreateOrderRequest request) {
        Order order = orderRepository.save(
                new Order(request.customerId(), request.productId(), request.quantity(), request.amount()));
        OrderSaga saga = sagaRepository.save(new OrderSaga(order.getId(), Instant.now().plus(timeouts.payment())));
        outbox.write(Topics.PAYMENT_COMMANDS, order.getId().toString(),
                new ProcessPayment(saga.getId(), order.getId(), order.getCustomerId(), order.getAmount()));
        return OrderResponse.of(order, saga);
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> recent(int limit) {
        List<Order> orders = orderRepository.findAll(PageRequest.of(0, limit, Sort.by(Sort.Direction.DESC, "createdAt")))
                .getContent();
        Map<UUID, OrderSaga> sagas = sagaRepository.findByOrderIdIn(orders.stream().map(Order::getId).toList()).stream()
                .collect(Collectors.toMap(OrderSaga::getOrderId, Function.identity()));
        return orders.stream()
                .filter(order -> sagas.containsKey(order.getId()))
                .map(order -> OrderResponse.of(order, sagas.get(order.getId())))
                .toList();
    }

    @Transactional(readOnly = true)
    public Optional<OrderResponse> find(UUID orderId) {
        return orderRepository.findById(orderId)
                .flatMap(order -> sagaRepository.findByOrderId(orderId).map(saga -> OrderResponse.of(order, saga)));
    }
}
