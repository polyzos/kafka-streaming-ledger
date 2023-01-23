package io.ipolyzos.failover

import arrow.core.Either
import arrow.fx.coroutines.CircuitBreaker
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlinx.coroutines.delay

@ExperimentalTime
suspend fun main(): Unit {
    val circuitBreaker = CircuitBreaker.of(
        maxFailures = 2,
        resetTimeout = Duration.seconds(2),
        exponentialBackoffFactor = 1.2,
        maxResetTimeout = Duration.seconds(60),
    )
    circuitBreaker.protectOrThrow { "I am in Closed: ${circuitBreaker.state()}" }.also(::println)

    println("Service getting overloaded . . .")

    Either.catch { circuitBreaker.protectOrThrow { throw RuntimeException("Service overloaded") } }.also(::println)
    Either.catch { circuitBreaker.protectOrThrow { throw RuntimeException("Service overloaded") } }.also(::println)
    circuitBreaker.protectEither { }.also { println("I am Open and short-circuit with ${it}. ${circuitBreaker.state()}") }

    println("Service recovering . . .").also { delay(2000) }

    circuitBreaker.protectOrThrow { "I am running test-request in HalfOpen: ${circuitBreaker.state()}" }.also(::println)
    println("I am back to normal state closed ${circuitBreaker.state()}")


    println("my test")
    val res1 = circuitBreaker.protectEither { "ti kanw edw?" }.also(::println)
    println(res1)
    val res2 =
        Either.catch { circuitBreaker.protectOrThrow { throw RuntimeException("Service overloaded") } }.also(::println)

    circuitBreaker.protectEither { }.also { println("I am Open and short-circuit with ${it}. ${circuitBreaker.state()}") }

}
