package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        const val PAYMENT_WORKERS = 16
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentAccounts: List<PaymentExternalSystemAdapter>

    private val processingTimeMs by lazy {
        paymentAccounts.sumOf { it.averageProcessingTime().toMillis() }.coerceAtLeast(1)
    }

    private val paymentExecutor by lazy {
        val accountThroughput = paymentAccounts.minOf { it.throughputPerSec() }
        val executorThroughput = (PAYMENT_WORKERS * 1_000L / processingTimeMs).coerceAtLeast(1)
        val effectiveThroughput = minOf(accountThroughput, executorThroughput)
        val queueCapacity = ((effectiveThroughput * processingTimeMs + 999) / 1_000).toInt().coerceAtLeast(1)

        ThreadPoolExecutor(
            PAYMENT_WORKERS,
            PAYMENT_WORKERS,
            0L,
            TimeUnit.MILLISECONDS,
            ArrayBlockingQueue(queueCapacity),
            NamedThreadFactory("payment-submission-executor"),
            ThreadPoolExecutor.AbortPolicy()
        )
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        if (deadline - createdAt <= processingTimeMs) return null

        try {
            paymentExecutor.submit {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
        } catch (_: RejectedExecutionException) {
            return null
        }

        return createdAt
    }
}
