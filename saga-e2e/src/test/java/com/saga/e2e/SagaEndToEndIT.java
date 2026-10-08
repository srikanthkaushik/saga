package com.saga.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Drives the full saga through the real service jars. Each test seeds its own customer/product rows
 * with unique ids so tests stay independent of each other and of the Flyway seed data.
 */
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SagaEndToEndIT {

    /**
     * Stall tests poison every partition of a topic; run them after the DLT-replay tests, which would
     * otherwise replay those poison records and stall later tests.
     */
    private static final int AFTER_EVERYTHING_ELSE = Integer.MAX_VALUE;

    private static final Duration SAGA_TIMEOUT = Duration.ofSeconds(30);

    /** Backoff used by the services under test: 1s, 2s, 4s -> at least 7s before dead-lettering. */
    private static final int RETRIES = 3;
    private static final Duration RETRY_INITIAL_INTERVAL = Duration.ofSeconds(1);
    private static final Duration MIN_RETRY_BUDGET = Duration.ofSeconds(7);

    /** Saga step timeout for the services under test; well below MIN_RETRY_BUDGET so a stall forces a timeout. */
    private static final Duration STEP_TIMEOUT = Duration.ofSeconds(3);
    private static final int PARTITIONS = 3;
    private static final int STUCK_AFTER_RESENDS = 2;
    private static final Duration DLT_TIMEOUT = Duration.ofSeconds(30);

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17")
            .withUsername("saga")
            .withPassword("saga")
            .withDatabaseName("saga")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of(System.getProperty("saga.e2e.init-sql"))),
                    "/docker-entrypoint-initdb.d/init.sql");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final List<ServiceProcess> SERVICES = new ArrayList<>();
    private static ServiceProcess orderService;
    private static ServiceProcess inventoryService;
    private static KafkaProbe kafka;

    @BeforeAll
    static void startServices() throws Exception {
        Path logDir = Path.of(System.getProperty("saga.e2e.log-dir"));
        orderService = launch("order-service", "saga.e2e.order-jar", "order_db", logDir);
        launch("payment-service", "saga.e2e.payment-jar", "payment_db", logDir);
        inventoryService = launch("inventory-service", "saga.e2e.inventory-jar", "inventory_db", logDir);
        for (ServiceProcess service : SERVICES) {
            service.awaitHealthy(HTTP);
        }
        kafka = new KafkaProbe(KAFKA.getBootstrapServers());
    }

    @AfterAll
    static void stopServices() throws InterruptedException {
        if (kafka != null) {
            kafka.close();
        }
        for (ServiceProcess service : SERVICES) {
            service.close();
        }
    }

    @Test
    void happyPath_approvesOrder_debitsCreditAndReservesStock() throws Exception {
        String customer = seedCustomer("1000.00");
        String product = seedProduct(10);

        UUID orderId = placeOrder(customer, product, 3, "250.00");
        JsonNode order = awaitTerminal(orderId);

        assertThat(order.get("status").asString()).isEqualTo("APPROVED");
        assertThat(order.get("sagaState").asString()).isEqualTo("COMPLETED");
        assertThat(credit(customer)).isEqualByComparingTo("750.00");
        assertThat(paymentStatus(orderId)).isEqualTo("COMPLETED");
        assertThat(stock(product)).isEqualTo(7);
        assertThat(reservationCount(orderId)).isEqualTo(1);
    }

    @Test
    void insufficientCredit_rejectsOrder_andNeverTouchesInventory() throws Exception {
        String customer = seedCustomer("50.00");
        String product = seedProduct(10);

        UUID orderId = placeOrder(customer, product, 1, "500.00");
        JsonNode order = awaitTerminal(orderId);

        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(order.get("sagaState").asString()).isEqualTo("FAILED");
        assertThat(order.get("rejectionReason").asString()).isEqualTo("Insufficient credit");
        assertThat(credit(customer)).isEqualByComparingTo("50.00");
        assertThat(paymentStatus(orderId)).isNull();
        assertThat(stock(product)).isEqualTo(10);
    }

    @Test
    void unknownCustomer_rejectsOrder() throws Exception {
        String product = seedProduct(10);

        UUID orderId = placeOrder("ghost-" + UUID.randomUUID(), product, 1, "10.00");
        JsonNode order = awaitTerminal(orderId);

        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(order.get("rejectionReason").asString()).startsWith("Unknown customer");
    }

    @Test
    void outOfStock_compensatesWithRefund_andRejectsOrder() throws Exception {
        String customer = seedCustomer("1000.00");
        String product = seedProduct(0);

        UUID orderId = placeOrder(customer, product, 1, "30.00");
        JsonNode order = awaitTerminal(orderId);

        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(order.get("sagaState").asString()).isEqualTo("FAILED");
        assertThat(order.get("rejectionReason").asString()).isEqualTo("Insufficient stock");
        assertThat(paymentStatus(orderId)).isEqualTo("REFUNDED");
        assertThat(credit(customer)).isEqualByComparingTo("1000.00");
        assertThat(stock(product)).isZero();
    }

    @Test
    void concurrentOrders_neverOversellStock_andRefundTheLosers() throws Exception {
        String customer = seedCustomer("1000.00");
        String product = seedProduct(50);
        int orders = 10;

        List<UUID> orderIds = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(orders)) {
            List<Future<UUID>> futures = new ArrayList<>();
            for (int i = 0; i < orders; i++) {
                futures.add(pool.submit(() -> placeOrder(customer, product, 10, "20.00")));
            }
            for (Future<UUID> future : futures) {
                orderIds.add(future.get());
            }
        }

        int approved = 0;
        int rejected = 0;
        for (UUID orderId : orderIds) {
            String status = awaitTerminal(orderId).get("status").asString();
            if (status.equals("APPROVED")) {
                approved++;
            } else {
                rejected++;
                assertThat(paymentStatus(orderId)).isEqualTo("REFUNDED");
            }
        }

        assertThat(approved).isEqualTo(5);
        assertThat(rejected).isEqualTo(5);
        assertThat(stock(product)).isZero();
        assertThat(credit(customer)).isEqualByComparingTo("900.00");
    }

    @Test
    void invalidRequest_isRejectedWith400() throws Exception {
        HttpResponse<String> response = postOrder("{\"customerId\":\"\",\"quantity\":0}");
        assertThat(response.statusCode()).isEqualTo(400);
    }

    // ---- dead-letter topics ---------------------------------------------------------------------

    @Test
    void unknownMessageType_skipsRetries_andIsDeadLetteredImmediately() throws Exception {
        String key = UUID.randomUUID().toString();
        Instant sent = Instant.now();
        UUID messageId = kafka.send("payment.commands", key, "NoSuchCommand", "{\"foo\":1}");

        ConsumerRecord<String, String> dead = kafka.awaitDeadLetter("payment.commands-dlt", messageId, DLT_TIMEOUT)
                .orElseThrow(() -> new AssertionError("message " + messageId + " never reached payment.commands-dlt"));

        assertThat(Duration.between(sent, Instant.now())).isLessThan(MIN_RETRY_BUDGET);
        assertThat(dead.key()).isEqualTo(key);
        assertThat(dead.value()).isEqualTo("{\"foo\":1}");
        assertThat(KafkaProbe.header(dead, "messageType")).isEqualTo("NoSuchCommand");
        assertThat(KafkaProbe.header(dead, "kafka_dlt-original-topic")).isEqualTo("payment.commands");
        assertThat(exceptionHeaders(dead)).contains("NonRetryableMessageException");
    }

    @Test
    void replyForUnknownSaga_isDeadLettered_andSagaFlowKeepsWorking() throws Exception {
        UUID sagaId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        Instant sent = Instant.now();
        UUID messageId = kafka.send("order.saga.replies", orderId.toString(), "PaymentProcessed",
                JSON.writeValueAsString(Map.of("sagaId", sagaId, "orderId", orderId)));

        ConsumerRecord<String, String> dead = kafka.awaitDeadLetter("order.saga.replies-dlt", messageId, DLT_TIMEOUT)
                .orElseThrow(() -> new AssertionError("reply " + messageId + " never reached order.saga.replies-dlt"));

        assertThat(Duration.between(sent, Instant.now())).isLessThan(MIN_RETRY_BUDGET);
        assertThat(exceptionHeaders(dead)).contains("Unknown saga " + sagaId);

        String customer = seedCustomer("100.00");
        String product = seedProduct(1);
        UUID healthyOrder = placeOrder(customer, product, 1, "10.00");
        assertThat(awaitTerminal(healthyOrder).get("status").asString()).isEqualTo("APPROVED");
    }

    @Test
    void transientHandlerFailure_isRetriedWithBackoff_thenDeadLettered() throws Exception {
        String customer = seedCustomer("100.00");
        UUID orderId = UUID.randomUUID();
        // amount=null decodes fine but blows up inside the handler with an NPE: a generic, retryable failure
        String json = "{\"sagaId\":\"" + UUID.randomUUID() + "\",\"orderId\":\"" + orderId
                + "\",\"customerId\":\"" + customer + "\",\"amount\":null}";
        Instant sent = Instant.now();
        UUID messageId = kafka.send("payment.commands", orderId.toString(), "ProcessPayment", json);

        ConsumerRecord<String, String> dead = kafka.awaitDeadLetter("payment.commands-dlt", messageId, DLT_TIMEOUT)
                .orElseThrow(() -> new AssertionError("command " + messageId + " never reached payment.commands-dlt"));

        assertThat(Duration.between(sent, Instant.now())).isGreaterThanOrEqualTo(MIN_RETRY_BUDGET);
        assertThat(KafkaProbe.header(dead, "kafka_dlt-original-topic")).isEqualTo("payment.commands");
        assertThat(exceptionHeaders(dead)).doesNotContain("NonRetryableMessageException");
        assertThat(credit(customer)).isEqualByComparingTo("100.00");
        assertThat(paymentStatus(orderId)).isNull();
    }

    // ---- DLT replay -----------------------------------------------------------------------------

    @Test
    void replay_reprocessesDeadLetter_onceTheCauseIsFixed() throws Exception {
        String product = seedProduct(5);
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();

        // A reply arrives before its saga exists -> dead-lettered
        UUID messageId = kafka.send("order.saga.replies", orderId.toString(), "PaymentProcessed",
                JSON.writeValueAsString(Map.of("sagaId", sagaId, "orderId", orderId)));
        kafka.awaitDeadLetter("order.saga.replies-dlt", messageId, DLT_TIMEOUT)
                .orElseThrow(() -> new AssertionError("reply " + messageId + " never dead-lettered"));
        assertThat(dltStatus(orderService, "order.saga.replies").get("pending").asLong()).isPositive();

        // Fix the cause: the saga now exists, waiting for exactly that reply
        seedSaga(orderId, sagaId, "PAYMENT_PENDING", "replay-customer", product);

        JsonNode result = replay(orderService, "order.saga.replies", "{}");
        assertThat(result.get("replayed").asInt()).isGreaterThanOrEqualTo(1);
        assertThat(result.get("pending").asLong()).isZero();

        JsonNode order = awaitTerminal(orderId);
        assertThat(order.get("status").asString()).isEqualTo("APPROVED");
        assertThat(order.get("sagaState").asString()).isEqualTo("COMPLETED");
        assertThat(stock(product)).isEqualTo(4);
    }

    @Test
    void replay_ofStillBrokenMessage_landsBackOnDlt_withReplayCount() throws Exception {
        UUID messageId = kafka.send("inventory.commands", UUID.randomUUID().toString(), "NoSuchCommand", "{}");
        kafka.awaitDeadLetter("inventory.commands-dlt", messageId, DLT_TIMEOUT)
                .orElseThrow(() -> new AssertionError("command " + messageId + " never dead-lettered"));

        JsonNode result = replay(inventoryService, "inventory.commands", "{\"limit\":1000}");
        assertThat(result.get("replayed").asInt()).isGreaterThanOrEqualTo(1);

        ConsumerRecord<String, String> again = kafka.awaitDeadLetter("inventory.commands-dlt", messageId, DLT_TIMEOUT,
                        record -> "1".equals(KafkaProbe.header(record, "dltReplayCount")))
                .orElseThrow(() -> new AssertionError("replayed command " + messageId + " did not return to the DLT"));
        assertThat(KafkaProbe.header(again, "messageType")).isEqualTo("NoSuchCommand");
        assertThat(exceptionHeaders(again)).contains("NonRetryableMessageException");
    }

    @Test
    void dltEndpoint_onlyServesTopicsTheServiceConsumes() throws Exception {
        HttpResponse<String> list = HTTP.send(
                HttpRequest.newBuilder(URI.create(orderService.baseUrl() + "/actuator/dlt")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(list.statusCode()).isEqualTo(200);
        JsonNode dlts = JSON.readTree(list.body());
        assertThat(dlts).hasSize(1);
        assertThat(dlts.get(0).get("dltTopic").asString()).isEqualTo("order.saga.replies-dlt");

        HttpResponse<String> foreign = HTTP.send(HttpRequest.newBuilder(
                                URI.create(orderService.baseUrl() + "/actuator/dlt/payment.commands"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(foreign.statusCode()).isEqualTo(400);

        HttpResponse<String> badLimit = HTTP.send(HttpRequest.newBuilder(
                                URI.create(orderService.baseUrl() + "/actuator/dlt/order.saga.replies"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"limit\":0}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(badLimit.statusCode()).isEqualTo(400);
    }

    // ---- saga timeouts + fencing ----------------------------------------------------------------

    @Test
    @Order(AFTER_EVERYTHING_ELSE)
    void paymentTimeout_refundsTheLateCharge_andRejectsOrder() throws Exception {
        String customer = seedCustomer("100.00");
        String product = seedProduct(5);
        String poisonCustomer = seedCustomer("100.00");
        // amount=null -> NPE inside the handler -> retryable -> each partition stalls for the retry budget
        stallAllPartitions("payment.commands", "ProcessPayment", () -> {
            Map<String, Object> poison = new HashMap<>();
            poison.put("sagaId", UUID.randomUUID());
            poison.put("orderId", UUID.randomUUID());
            poison.put("customerId", poisonCustomer);
            poison.put("amount", null);
            return JSON.writeValueAsString(poison);
        });

        UUID orderId = placeOrder(customer, product, 1, "40.00");
        JsonNode order = awaitTerminal(orderId);

        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(order.get("sagaState").asString()).isEqualTo("FAILED");
        assertThat(order.get("rejectionReason").asString()).isEqualTo("Payment timed out");
        // The stalled ProcessPayment still ran after the timeout - and the queued refund undid it
        assertThat(paymentStatus(orderId)).isEqualTo("REFUNDED");
        assertThat(credit(customer)).isEqualByComparingTo("100.00");
        assertThat(stock(product)).isEqualTo(5);
    }

    @Test
    @Order(AFTER_EVERYTHING_ELSE)
    void inventoryTimeout_releasesTheLateReservation_refunds_andRejectsOrder() throws Exception {
        String customer = seedCustomer("100.00");
        String product = seedProduct(5);
        String poisonProduct = seedProduct(5);
        // quantity=-1 passes the stock check, then violates reservation's CHECK (quantity > 0) -> retryable
        stallAllPartitions("inventory.commands", "ReserveInventory", () -> JSON.writeValueAsString(Map.of(
                "sagaId", UUID.randomUUID(), "orderId", UUID.randomUUID(), "productId", poisonProduct, "quantity", -1)));

        UUID orderId = placeOrder(customer, product, 2, "40.00");
        JsonNode order = awaitTerminal(orderId);

        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(order.get("sagaState").asString()).isEqualTo("FAILED");
        assertThat(order.get("rejectionReason").asString()).isEqualTo("Inventory timed out");
        // The stalled ReserveInventory still ran after the timeout - and the queued release undid it
        assertThat(reservationStatus(orderId)).isEqualTo("RELEASED");
        assertThat(stock(product)).isEqualTo(5);
        assertThat(paymentStatus(orderId)).isEqualTo("REFUNDED");
        assertThat(credit(customer)).isEqualByComparingTo("100.00");
    }

    @Test
    void refundBeforeCharge_leavesTombstone_andRefusesTheLateCharge() throws Exception {
        String customer = seedCustomer("100.00");
        String product = seedProduct(5);
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        seedSaga(orderId, sagaId, "PAYMENT_PENDING", customer, product);

        kafka.send("payment.commands", orderId.toString(), "RefundPayment",
                JSON.writeValueAsString(Map.of("sagaId", sagaId, "orderId", orderId)));
        kafka.send("payment.commands", orderId.toString(), "ProcessPayment", JSON.writeValueAsString(Map.of(
                "sagaId", sagaId, "orderId", orderId, "customerId", customer, "amount", new BigDecimal("10.00"))));

        JsonNode order = awaitTerminal(orderId);
        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(order.get("rejectionReason").asString()).isEqualTo("Order already CANCELLED");
        assertThat(paymentStatus(orderId)).isEqualTo("CANCELLED");
        assertThat(credit(customer)).isEqualByComparingTo("100.00");
    }

    @Test
    void releaseBeforeReserve_leavesTombstone_andRefusesTheLateReservation() throws Exception {
        String customer = seedCustomer("100.00");
        String product = seedProduct(5);
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        seedSaga(orderId, sagaId, "INVENTORY_PENDING", customer, product);

        kafka.send("inventory.commands", orderId.toString(), "ReleaseInventory", JSON.writeValueAsString(Map.of(
                "sagaId", sagaId, "orderId", orderId, "productId", product, "quantity", 1)));
        kafka.send("inventory.commands", orderId.toString(), "ReserveInventory", JSON.writeValueAsString(Map.of(
                "sagaId", sagaId, "orderId", orderId, "productId", product, "quantity", 1)));

        JsonNode order = awaitTerminal(orderId);
        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(order.get("rejectionReason").asString()).isEqualTo("Order already released");
        assertThat(reservationStatus(orderId)).isEqualTo("RELEASED");
        assertThat(stock(product)).isEqualTo(5);
    }

    // ---- stuck compensation alerting ------------------------------------------------------------

    @Test
    @Order(AFTER_EVERYTHING_ELSE)
    void failingRefund_isReportedAsStuck_andClearsOnceTheRefundSucceeds() throws Exception {
        String customer = seedCustomer("100.00");
        String product = seedProduct(0);  // out of stock -> InventoryFailed -> refund
        UUID orderId;

        breakRefundsFor(customer);
        try {
            orderId = placeOrder(customer, product, 1, "25.00");

            JsonNode stuck = await("saga for order " + orderId + " to be reported stuck")
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> stuckSagaFor(orderId), node -> node != null);
            assertThat(stuck.get("state").asString()).isEqualTo("COMPENSATING");
            assertThat(stuck.get("failureReason").asString()).isEqualTo("Insufficient stock");
            assertThat(stuck.get("compensationResends").asInt()).isGreaterThanOrEqualTo(STUCK_AFTER_RESENDS);
            assertThat(stuck.get("compensatingSince").isNull()).isFalse();

            await("stuck gauge").atMost(Duration.ofSeconds(10))
                    .until(() -> metricValue("saga.compensation.stuck"), value -> value >= 1);
            assertThat(metricValue("saga.compensation.resends")).isGreaterThanOrEqualTo(STUCK_AFTER_RESENDS);
            assertThat(get(orderService, "/actuator/prometheus"))
                    .contains("saga_compensation_stuck ")
                    .contains("saga_compensation_oldest_age_seconds ")
                    .contains("saga_compensation_resends_total ");

            // Customer is charged for a rejected order until the refund gets through
            assertThat(getOrder(orderId).get("status").asString()).isEqualTo("PENDING");
            assertThat(paymentStatus(orderId)).isEqualTo("COMPLETED");
            assertThat(credit(customer)).isEqualByComparingTo("75.00");
        } finally {
            fixRefunds();
        }

        JsonNode order = awaitTerminal(orderId);
        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(order.get("rejectionReason").asString()).isEqualTo("Insufficient stock");
        assertThat(paymentStatus(orderId)).isEqualTo("REFUNDED");
        assertThat(credit(customer)).isEqualByComparingTo("100.00");
        assertThat(stuckSagaFor(orderId)).isNull();
    }

    // ---- manual resolution ----------------------------------------------------------------------

    @Test
    @Order(AFTER_EVERYTHING_ELSE)
    void retry_resendsCompensation_resetsStuckCount_andIsAudited() throws Exception {
        String customer = seedCustomer("100.00");
        String product = seedProduct(0);
        String trigger = blockRefundRepliesFor(customer);
        UUID orderId;
        String sagaId;
        try {
            orderId = placeOrder(customer, product, 1, "25.00");
            sagaId = awaitStuck(orderId).get("sagaId").asString();

            HttpResponse<String> response = intervene(sagaId, "{\"action\":\"retry\",\"operator\":\"bob\"}");
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
            JsonNode result = JSON.readTree(response.body());
            assertThat(result.get("action").asString()).isEqualTo("RETRY");
            assertThat(result.get("fromState").asString()).isEqualTo("COMPENSATING");
            assertThat(result.get("toState").asString()).isEqualTo("COMPENSATING");
            assertThat(result.get("compensationResends").asInt()).isZero();
            assertThat(result.get("operator").asString()).isEqualTo("bob");
        } finally {
            unblock(trigger);
        }

        JsonNode order = awaitTerminal(orderId);
        assertThat(order.get("status").asString()).isEqualTo("REJECTED");
        assertThat(paymentStatus(orderId)).isEqualTo("REFUNDED");
        assertThat(credit(customer)).isEqualByComparingTo("100.00");

        JsonNode history = sagaDetail(sagaId).get("interventions");
        assertThat(history).hasSize(1);
        assertThat(history.get(0).get("action").asString()).isEqualTo("RETRY");
        assertThat(history.get(0).get("operator").asString()).isEqualTo("bob");
    }

    @Test
    @Order(AFTER_EVERYTHING_ELSE)
    void resolve_afterManualRefund_closesSaga_andLateCompensationDoesNotRefundTwice() throws Exception {
        String customer = seedCustomer("100.00");
        String product = seedProduct(0);
        String trigger = blockRefundRepliesFor(customer);
        UUID orderId;
        String sagaId;
        try {
            orderId = placeOrder(customer, product, 1, "25.00");
            sagaId = awaitStuck(orderId).get("sagaId").asString();
            assertThat(credit(customer)).isEqualByComparingTo("75.00");

            // Operator refunds by hand, in payment-service's own records
            manualRefund(orderId);
            assertThat(credit(customer)).isEqualByComparingTo("100.00");

            assertThat(intervene(sagaId, "{\"action\":\"resolve\",\"operator\":\"alice\"}").statusCode())
                    .as("resolve without note").isEqualTo(400);

            HttpResponse<String> response = intervene(sagaId,
                    "{\"action\":\"resolve\",\"operator\":\"alice\",\"note\":\"Refunded by hand, ticket OPS-123\"}");
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
            JsonNode result = JSON.readTree(response.body());
            assertThat(result.get("fromState").asString()).isEqualTo("COMPENSATING");
            assertThat(result.get("toState").asString()).isEqualTo("FAILED");
            assertThat(result.get("orderStatus").asString()).isEqualTo("REJECTED");

            JsonNode order = getOrder(orderId);
            assertThat(order.get("status").asString()).isEqualTo("REJECTED");
            assertThat(order.get("sagaState").asString()).isEqualTo("FAILED");
            assertThat(order.get("rejectionReason").asString())
                    .isEqualTo("Insufficient stock (compensation resolved manually)");
            assertThat(stuckSagaFor(orderId)).isNull();
            assertThat(intervene(sagaId, "{\"action\":\"resolve\",\"operator\":\"alice\",\"note\":\"again\"}").statusCode())
                    .as("resolve on a FAILED saga").isEqualTo(409);
        } finally {
            unblock(trigger);
        }

        // The refund still queued in payment-service now completes - as a no-op, because the payment is REFUNDED
        await("late PaymentRefunded for " + orderId).atMost(DLT_TIMEOUT)
                .until(() -> paymentReplies(orderId, "PaymentRefunded"), count -> count >= 1);
        assertThat(credit(customer)).isEqualByComparingTo("100.00");
        assertThat(getOrder(orderId).get("sagaState").asString()).isEqualTo("FAILED");

        JsonNode detail = sagaDetail(sagaId);
        assertThat(detail.get("state").asString()).isEqualTo("FAILED");
        assertThat(detail.get("interventions")).hasSize(1);
        assertThat(detail.get("interventions").get(0).get("note").asString()).isEqualTo("Refunded by hand, ticket OPS-123");
    }

    @Test
    void intervention_rejectsBadRequests() throws Exception {
        String unknown = UUID.randomUUID().toString();
        assertThat(intervene(unknown, "{\"action\":\"retry\",\"operator\":\"bob\"}").statusCode()).isEqualTo(404);
        assertThat(intervene("not-a-uuid", "{\"action\":\"retry\",\"operator\":\"bob\"}").statusCode()).isEqualTo(400);
        assertThat(intervene(unknown, "{\"action\":\"explode\",\"operator\":\"bob\"}").statusCode()).isEqualTo(400);
        assertThat(intervene(unknown, "{\"action\":\"retry\"}").statusCode()).isEqualTo(400);
        assertThat(HTTP.send(HttpRequest.newBuilder(URI.create(orderService.baseUrl() + "/actuator/stucksagas/" + unknown)).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);

        // Not compensating -> 409
        UUID orderId = placeOrder(seedCustomer("100.00"), seedProduct(5), 1, "10.00");
        awaitTerminal(orderId);
        String sagaId = getOrder(orderId).get("sagaId").asString();
        assertThat(intervene(sagaId, "{\"action\":\"retry\",\"operator\":\"bob\"}").statusCode()).isEqualTo(409);
    }

    private static JsonNode awaitStuck(UUID orderId) {
        return await("saga for order " + orderId + " to be reported stuck")
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> stuckSagaFor(orderId), node -> node != null);
    }

    private static HttpResponse<String> intervene(String sagaId, String body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(orderService.baseUrl() + "/actuator/stucksagas/" + sagaId))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode sagaDetail(String sagaId) throws Exception {
        return JSON.readTree(get(orderService, "/actuator/stucksagas/" + sagaId));
    }

    /**
     * Simulates payment-service being unable to publish refund confirmations for one customer: every refund
     * handler run rolls back at the outbox insert (retryable), regardless of the payment row's state.
     */
    private static String blockRefundRepliesFor(String customerId) throws SQLException {
        String trigger = "e2e_block_reply_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection c = connect("payment_db"); var st = c.createStatement()) {
            st.execute("""
                    create or replace function e2e_block_reply() returns trigger language plpgsql as $$
                    begin
                        if exists (select 1 from payment where order_id = new.message_key::uuid and customer_id = tg_argv[0]) then
                            raise exception 'e2e: PaymentRefunded blocked for order %', new.message_key;
                        end if;
                        return new;
                    end $$
                    """);
            st.execute("""
                    create trigger %s before insert on outbox for each row
                    when (new.message_type = 'PaymentRefunded')
                    execute function e2e_block_reply('%s')
                    """.formatted(trigger, customerId));
        }
        return trigger;
    }

    private static void unblock(String trigger) throws SQLException {
        try (Connection c = connect("payment_db"); var st = c.createStatement()) {
            st.execute("drop trigger if exists " + trigger + " on outbox");
        }
    }

    /** What an operator would do by hand in payment_db: credit the customer back and mark the payment refunded. */
    private static void manualRefund(UUID orderId) throws SQLException {
        try (Connection c = connect("payment_db")) {
            c.setAutoCommit(false);
            try (PreparedStatement credit = c.prepareStatement("""
                    update customer_credit cc set available_credit = cc.available_credit + p.amount
                    from payment p where p.order_id = ? and p.status = 'COMPLETED' and cc.customer_id = p.customer_id
                    """);
                 PreparedStatement payment = c.prepareStatement(
                         "update payment set status = 'REFUNDED', updated_at = now() where order_id = ? and status = 'COMPLETED'")) {
                credit.setObject(1, orderId);
                assertThat(credit.executeUpdate()).isEqualTo(1);
                payment.setObject(1, orderId);
                assertThat(payment.executeUpdate()).isEqualTo(1);
                c.commit();
            }
        }
    }

    private static int paymentReplies(UUID orderId, String messageType) throws SQLException {
        return querySingle("payment_db", "select count(*) from outbox where message_key = ? and message_type = '" + messageType + "'",
                orderId.toString(), rs -> rs.getInt(1));
    }

    /** Simulates a payment-service fault for one customer: every refund for them fails (retryable). */
    private static void breakRefundsFor(String customerId) throws SQLException {
        try (Connection c = connect("payment_db"); var st = c.createStatement()) {
            st.execute("""
                    create or replace function e2e_fail_refund() returns trigger language plpgsql as $$
                    begin
                        raise exception 'e2e: refunds unavailable for %', new.customer_id;
                    end $$
                    """);
            st.execute("""
                    create trigger e2e_fail_refund before update on payment for each row
                    when (new.status = 'REFUNDED' and new.customer_id = '%s')
                    execute function e2e_fail_refund()
                    """.formatted(customerId));
        }
    }

    private static void fixRefunds() throws SQLException {
        try (Connection c = connect("payment_db"); var st = c.createStatement()) {
            st.execute("drop trigger if exists e2e_fail_refund on payment");
        }
    }

    private static JsonNode stuckSagaFor(UUID orderId) throws Exception {
        for (JsonNode saga : JSON.readTree(get(orderService, "/actuator/stucksagas"))) {
            if (saga.get("orderId").asString().equals(orderId.toString())) {
                return saga;
            }
        }
        return null;
    }

    private static double metricValue(String name) throws Exception {
        JsonNode metric = JSON.readTree(get(orderService, "/actuator/metrics/" + name));
        for (JsonNode measurement : metric.get("measurements")) {
            if (measurement.get("statistic").asString().equals("VALUE")
                    || measurement.get("statistic").asString().equals("COUNT")) {
                return measurement.get("value").asDouble();
            }
        }
        throw new AssertionError("No VALUE/COUNT measurement for " + name + ": " + metric);
    }

    private static String get(ServiceProcess service, String path) throws Exception {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(service.baseUrl() + path)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(path + " -> " + response.body()).isEqualTo(200);
        return response.body();
    }

    private static JsonNode dltStatus(ServiceProcess service, String topic) throws Exception {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(service.baseUrl() + "/actuator/dlt/" + topic)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private static JsonNode replay(ServiceProcess service, String topic, String body) throws Exception {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(
                                URI.create(service.baseUrl() + "/actuator/dlt/" + topic))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    /** Inserts an order + saga directly, with no deadline so the timeout scanner leaves it alone. */
    private static void seedSaga(UUID orderId, UUID sagaId, String state, String customerId, String productId)
            throws SQLException {
        try (Connection c = connect("order_db")) {
            try (PreparedStatement ps = c.prepareStatement("""
                    insert into orders (id, customer_id, product_id, quantity, amount, status, version, created_at, updated_at)
                    values (?, ?, ?, 1, 10.00, 'PENDING', 0, now(), now())
                    """)) {
                ps.setObject(1, orderId);
                ps.setString(2, customerId);
                ps.setString(3, productId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    insert into order_saga (id, order_id, state, version, created_at, updated_at)
                    values (?, ?, ?, 0, now(), now())
                    """)) {
                ps.setObject(1, sagaId);
                ps.setObject(2, orderId);
                ps.setString(3, state);
                ps.executeUpdate();
            }
        }
    }

    /**
     * Puts one retryable poison record on every partition of {@code topic}: each consumer thread then sits in
     * blocking retry for the full backoff (>= {@link #MIN_RETRY_BUDGET}) and everything sent after it waits.
     */
    private static void stallAllPartitions(String topic, String messageType, Supplier<String> poisonJson) throws Exception {
        for (int partition = 0; partition < PARTITIONS; partition++) {
            kafka.send(topic, partition, UUID.randomUUID().toString(), messageType, poisonJson.get());
        }
    }

    private static String exceptionHeaders(ConsumerRecord<String, String> record) {
        return String.join(" | ",
                String.valueOf(KafkaProbe.header(record, "kafka_dlt-exception-fqcn")),
                String.valueOf(KafkaProbe.header(record, "kafka_dlt-exception-cause-fqcn")),
                String.valueOf(KafkaProbe.header(record, "kafka_dlt-exception-message")));
    }

    // ---- service launch -------------------------------------------------------------------------

    private static ServiceProcess launch(String name, String jarProperty, String database, Path logDir) throws Exception {
        ServiceProcess service = ServiceProcess.start(name, Path.of(System.getProperty(jarProperty)), logDir, Map.ofEntries(
                Map.entry("spring.datasource.url", jdbcUrl(database)),
                Map.entry("spring.datasource.username", POSTGRES.getUsername()),
                Map.entry("spring.datasource.password", POSTGRES.getPassword()),
                Map.entry("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers()),
                Map.entry("saga.outbox.poll-interval", "100ms"),
                Map.entry("saga.kafka.retry.max-retries", String.valueOf(RETRIES)),
                Map.entry("saga.kafka.retry.initial-interval", RETRY_INITIAL_INTERVAL.toMillis() + "ms"),
                Map.entry("saga.kafka.retry.multiplier", "2.0"),
                Map.entry("saga.timeout.payment", STEP_TIMEOUT.toMillis() + "ms"),
                Map.entry("saga.timeout.inventory", STEP_TIMEOUT.toMillis() + "ms"),
                Map.entry("saga.timeout.compensation", STEP_TIMEOUT.toMillis() + "ms"),
                Map.entry("saga.timeout.scan-interval", "250ms"),
                Map.entry("saga.alert.stuck-after-resends", String.valueOf(STUCK_AFTER_RESENDS)),
                Map.entry("saga.alert.refresh-interval", "500ms")));
        SERVICES.add(service);
        return service;
    }

    // ---- HTTP -----------------------------------------------------------------------------------

    private static UUID placeOrder(String customerId, String productId, int quantity, String amount) throws Exception {
        String body = JSON.writeValueAsString(Map.of(
                "customerId", customerId,
                "productId", productId,
                "quantity", quantity,
                "amount", new BigDecimal(amount)));
        HttpResponse<String> response = postOrder(body);
        assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
        return UUID.fromString(JSON.readTree(response.body()).get("id").asString());
    }

    private static HttpResponse<String> postOrder(String body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(orderService.baseUrl() + "/orders"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode getOrder(UUID orderId) throws Exception {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(orderService.baseUrl() + "/orders/" + orderId)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private static JsonNode awaitTerminal(UUID orderId) {
        return await("order " + orderId + " to finish")
                .atMost(SAGA_TIMEOUT)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> getOrder(orderId), order -> !order.get("status").asString().equals("PENDING"));
    }

    // ---- JDBC -----------------------------------------------------------------------------------

    private static String jdbcUrl(String database) {
        return "jdbc:postgresql://" + POSTGRES.getHost() + ":" + POSTGRES.getMappedPort(5432) + "/" + database;
    }

    private static Connection connect(String database) throws SQLException {
        return DriverManager.getConnection(jdbcUrl(database), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static String seedCustomer(String credit) throws SQLException {
        String customerId = "customer-" + UUID.randomUUID();
        try (Connection c = connect("payment_db");
             PreparedStatement ps = c.prepareStatement(
                     "insert into customer_credit (customer_id, available_credit) values (?, ?)")) {
            ps.setString(1, customerId);
            ps.setBigDecimal(2, new BigDecimal(credit));
            ps.executeUpdate();
        }
        return customerId;
    }

    private static String seedProduct(int quantity) throws SQLException {
        String productId = "product-" + UUID.randomUUID();
        try (Connection c = connect("inventory_db");
             PreparedStatement ps = c.prepareStatement(
                     "insert into product (product_id, available_quantity) values (?, ?)")) {
            ps.setString(1, productId);
            ps.setInt(2, quantity);
            ps.executeUpdate();
        }
        return productId;
    }

    private static BigDecimal credit(String customerId) throws SQLException {
        return querySingle("payment_db", "select available_credit from customer_credit where customer_id = ?",
                customerId, rs -> rs.getBigDecimal(1));
    }

    private static String paymentStatus(UUID orderId) throws SQLException {
        return querySingle("payment_db", "select status from payment where order_id = ?",
                orderId, rs -> rs.getString(1));
    }

    private static int stock(String productId) throws SQLException {
        return querySingle("inventory_db", "select available_quantity from product where product_id = ?",
                productId, rs -> rs.getInt(1));
    }

    private static String reservationStatus(UUID orderId) throws SQLException {
        return querySingle("inventory_db", "select status from reservation where order_id = ?",
                orderId, rs -> rs.getString(1));
    }

    private static int reservationCount(UUID orderId) throws SQLException {
        return querySingle("inventory_db", "select count(*) from reservation where order_id = ?",
                orderId, rs -> rs.getInt(1));
    }

    private static <T> T querySingle(String database, String sql, Object param, RowMapper<T> mapper) throws SQLException {
        try (Connection c = connect(database); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? mapper.map(rs) : null;
            }
        }
    }

    @FunctionalInterface
    private interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }
}
