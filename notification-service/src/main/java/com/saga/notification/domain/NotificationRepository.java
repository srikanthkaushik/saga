package com.saga.notification.domain;

import java.util.List;
import java.util.UUID;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationRepository extends JpaRepository<Notification, UUID> {

    boolean existsByOrderIdAndType(UUID orderId, NotificationType type);

    List<Notification> findByOrderIdOrderBySentAt(UUID orderId);

    List<Notification> findAllByOrderBySentAtDesc(Pageable page);
}
