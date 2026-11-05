package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class PacedRateLimiter(
    private val interval: Duration,
    maxWaitingRequests: Int,
) : AutoCloseable {
    private val waitingSlots = Semaphore(maxWaitingRequests)
    private val scheduler = ScheduledThreadPoolExecutor(1).apply {
        removeOnCancelPolicy = true
    }
    private var nextPermitAt = System.nanoTime()

    init {
        require(!interval.isNegative && !interval.isZero)
        require(maxWaitingRequests > 0)
    }

    enum class AdmissionResult {
        ADMITTED,
        DEADLINE,
        FULL,
        TIMEOUT,
        INTERRUPTED,
    }

    @Synchronized
    fun acquire(timeout: Duration): CompletableFuture<AdmissionResult> {
        if (timeout.isNegative || timeout.isZero) {
            return CompletableFuture.completedFuture(AdmissionResult.DEADLINE)
        }
        if (scheduler.isShutdown) {
            return CompletableFuture.completedFuture(AdmissionResult.INTERRUPTED)
        }
        if (!waitingSlots.tryAcquire()) {
            return CompletableFuture.completedFuture(AdmissionResult.FULL)
        }

        val timeoutNs = timeout.toNanos()
        val now = System.nanoTime()
        val waitNs = (nextPermitAt - now).coerceAtLeast(0)
        if (waitNs >= timeoutNs) {
            waitingSlots.release()
            return CompletableFuture.completedFuture(AdmissionResult.TIMEOUT)
        }

        val result = CompletableFuture<AdmissionResult>()
        val expiresAt = now + timeoutNs
        nextPermitAt = now + waitNs + interval.toNanos()
        scheduler.schedule({
            val admittedAt = System.nanoTime()
            val outcome = if (admittedAt >= expiresAt) {
                AdmissionResult.TIMEOUT
            } else {
                AdmissionResult.ADMITTED
            }
            waitingSlots.release()
            result.complete(outcome)
        }, waitNs, TimeUnit.NANOSECONDS)
        return result
    }

    @Synchronized
    override fun close() {
        scheduler.shutdown()
    }
}
