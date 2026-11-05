package ru.quipy.apigateway

import jakarta.annotation.PreDestroy
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.PacedRateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.util.*
import java.time.Duration
import java.util.concurrent.CompletableFuture

@RestController
class APIController(
    private val orderRepository: OrderRepository,
    private val orderPayer: OrderPayer,
    paymentAccounts: List<PaymentExternalSystemAdapter>,
) {
    private val accountAverageProcessingTimeMs = paymentAccounts
        .sumOf { it.averageProcessingTime().toMillis() }
        .coerceAtLeast(1)
    private val paymentAdmission = PacedRateLimiter(
        interval = Duration.ofMillis(91),
        maxWaitingRequests = 288,
    )

    @PreDestroy
    fun closeAdmission() {
        paymentAdmission.close()
    }

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(
        @PathVariable orderId: UUID,
        @RequestParam deadline: Long,
    ): CompletableFuture<ResponseEntity<PaymentSubmissionDto>> {
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)
            ?: throw IllegalArgumentException("No such order $orderId")

        val remainingTimeMs = (deadline - System.currentTimeMillis()).coerceAtLeast(0)
        val averageProcessingTimeMs = (remainingTimeMs - accountAverageProcessingTimeMs).coerceAtLeast(0)
        val admissionWaitMs = (remainingTimeMs - averageProcessingTimeMs)
            .coerceIn(0, 24_000)
        return paymentAdmission.acquire(Duration.ofMillis(admissionWaitMs)).thenApplyAsync { admitted ->
            if (admitted != PacedRateLimiter.AdmissionResult.ADMITTED) {
                return@thenApplyAsync retryPayment()
            }

            val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
                ?: return@thenApplyAsync retryPayment()

            orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
        }
    }

    private fun retryPayment(): ResponseEntity<PaymentSubmissionDto> {
        return ResponseEntity.status(429)
            .header("Retry-After", (System.currentTimeMillis() + 100).toString())
            .build()
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
