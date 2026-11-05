package ru.quipy.apigateway

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PreDestroy
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.PacedRateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*
import java.time.Duration
import java.util.concurrent.CompletableFuture

@RestController
class APIController(
    private val orderRepository: OrderRepository,
    private val orderPayer: OrderPayer,
    private val meterRegistry: MeterRegistry,
) {
    private val admissionWaitTimer = meterRegistry.timer("payment.admission.wait")
    private val paymentAdmission = PacedRateLimiter(
        interval = Duration.ofMillis(91),
        maxWaitingRequests = 288,
        saturatedIntervalTimer = meterRegistry.timer("payment.admission.saturated.interval"),
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

        val admissionWaitMs = (deadline - System.currentTimeMillis() - 1_500)
            .coerceIn(0, 24_000)
        val waitSample = Timer.start(meterRegistry)
        return paymentAdmission.acquire(Duration.ofMillis(admissionWaitMs)).thenApplyAsync { admitted ->
            waitSample.stop(admissionWaitTimer)
            if (admitted != PacedRateLimiter.AdmissionResult.ADMITTED) {
                meterRegistry.counter(
                    "payment.admission.rejected",
                    "reason",
                    admitted.name.lowercase(Locale.ROOT),
                ).increment()
                return@thenApplyAsync retryPayment("admission")
            }

            val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
                ?: return@thenApplyAsync retryPayment("order_payer")

            orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
        }
    }

    private fun retryPayment(reason: String): ResponseEntity<PaymentSubmissionDto> {
        meterRegistry.counter("payment.submission.rejected", "reason", reason).increment()
        return ResponseEntity.status(429)
            .header("Retry-After", (System.currentTimeMillis() + 100).toString())
            .build()
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
