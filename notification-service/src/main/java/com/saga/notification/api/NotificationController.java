package com.saga.notification.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.saga.notification.domain.Notification;
import com.saga.notification.domain.NotificationRepository;
import com.saga.notification.domain.NotificationType;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private final NotificationRepository notificationRepository;

    public NotificationController(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    /** Notifications sent for one order (oldest first), or the most recent ones; {@code limit} is clamped to 1..200. */
    @GetMapping
    @Transactional(readOnly = true)
    public List<NotificationView> list(@RequestParam(required = false) UUID orderId,
                                       @RequestParam(defaultValue = "50") int limit) {
        List<Notification> notifications = orderId != null
                ? notificationRepository.findByOrderIdOrderBySentAt(orderId)
                : notificationRepository.findAllByOrderBySentAtDesc(PageRequest.of(0, Math.clamp(limit, 1, 200)));
        return notifications.stream().map(NotificationView::of).toList();
    }

    public record NotificationView(UUID id, UUID orderId, NotificationType type, String channel, String recipient,
                                   String subject, String body, Instant sentAt) {
        static NotificationView of(Notification n) {
            return new NotificationView(n.getId(), n.getOrderId(), n.getType(), n.getChannel(), n.getRecipient(),
                    n.getSubject(), n.getBody(), n.getSentAt());
        }
    }
}
