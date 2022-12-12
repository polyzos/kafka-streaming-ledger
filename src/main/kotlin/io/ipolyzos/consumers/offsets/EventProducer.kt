package io.ipolyzos.consumers.offsets

import io.ipolyzos.resources.ProducerResource
import io.ipolyzos.config.KafkaConfig

import io.ipolyzos.utils.LoggingUtils
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

fun main() = MessageProducer.runProducer()

object MessageProducer {
    fun runProducer() {
        with(LoggingUtils()) {
            val properties = KafkaConfig.buildProducerProps()

            val producerResource: ProducerResource<String, String> = ProducerResource.live(properties)

            val time = measureTimeMillis {
                for (i in 1 .. 10000) {
                    val key = (i % 10).toString()
                    val value = "msg-$i"
                    producerResource.produce(KafkaConfig.TEST_TOPIC, key, value)
                }
            }
            producerResource.flush()
            logger.info("Total time '${TimeUnit.MILLISECONDS.toSeconds(time)}' seconds")

            producerResource.shutdown()
        }
    }
}