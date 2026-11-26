package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        private const val MAX_PAYMENT_ATTEMPTS = 3
        private const val PAYMENT_ATTEMPT_TIMEOUT_MS = 1500L
        private const val TEMPORARY_ERROR_MESSAGE = "Temporary error"

        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )
    private val rateLimitWait = meterRegistry.timer("payment.rate.limit.wait", "account", accountName)
    private val httpRetries = meterRegistry.counter("payment.http.retries", "service", serviceName, "account", accountName)
    private val httpDuration = Timer.builder("payment.http.duration")
        .tags("service", serviceName, "account", accountName)
        .publishPercentiles(0.5, 0.7, 0.8, 0.85, 0.9, 0.99)
        .register(meterRegistry)
    private val inFlightRequests = Semaphore(parallelRequests, true)
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false).build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        repeat(MAX_PAYMENT_ATTEMPTS) { attempt ->
            val shouldRetry = performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, isRetry = attempt > 0)
            if (!shouldRetry) return
            if (deadline - now() <= requestAverageProcessingTime.toMillis()) return
        }
    }

    private fun performPaymentAttempt(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, isRetry: Boolean): Boolean {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        var submissionLogged = false
        var interrupted = false
        var slotAcquired = false
        var httpInProgress = false

        try {
            val windowWaitMs = deadline - now() - requestAverageProcessingTime.toMillis()
            if (windowWaitMs <= 0 || !inFlightRequests.tryAcquire(windowWaitMs, TimeUnit.MILLISECONDS)) {
                throw SocketTimeoutException("Payment deadline exceeded while waiting for account window")
            }
            slotAcquired = true

            val rateWaitMs = deadline - now() - requestAverageProcessingTime.toMillis()
            val permitAcquired = rateLimitWait.recordCallable {
                rateWaitMs > 0 && rateLimiter.tickBlocking(Duration.ofMillis(rateWaitMs))
            }
            if (permitAcquired != true) {
                throw SocketTimeoutException("Payment deadline exceeded while waiting for rate limit")
            }

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            submissionLogged = true
            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val remainingMs = deadline - now()
            if (remainingMs <= 0) {
                throw SocketTimeoutException("Payment deadline exceeded")
            }
            val attemptTimeoutMs = minOf(PAYMENT_ATTEMPT_TIMEOUT_MS, remainingMs)
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${Duration.ofMillis(attemptTimeoutMs)}")
                post(emptyBody)
            }.build()

            val call = client.newCall(request)
            call.timeout().timeout(attemptTimeoutMs, TimeUnit.MILLISECONDS)
            if (isRetry) httpRetries.increment()
            httpInProgress = true
            val (successful, body) = httpDuration.recordCallable {
                call.execute().use { response ->
                    if (response.code == 408) {
                        throw SocketTimeoutException("Payment provider returned HTTP 408")
                    }
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: InterruptedIOException) {
                        throw e
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }
                    response.isSuccessful to body
                }
            } ?: return false
            httpInProgress = false

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
            return successful && !body.result && body.message == TEMPORARY_ERROR_MESSAGE
        } catch (e: Exception) {
            interrupted = e is InterruptedException || Thread.currentThread().isInterrupted
            if (!submissionLogged) {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            }
            when (e) {
                is InterruptedIOException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
            return httpInProgress && e is InterruptedIOException && !interrupted
        } finally {
            if (slotAcquired) inFlightRequests.release()
            if (interrupted) Thread.currentThread().interrupt()
        }
    }

    override fun price() = properties.price

    override fun throughputPerSec() = rateLimitPerSec.toLong()

    override fun parallelRequests() = parallelRequests

    override fun averageProcessingTime() = requestAverageProcessingTime

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
