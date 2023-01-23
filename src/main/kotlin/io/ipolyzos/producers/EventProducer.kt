package io.ipolyzos.producers

import io.ipolyzos.resources.ProducerResource
import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.models.ClickEvent
import io.ipolyzos.utils.DataSourceUtils
import mu.KLogger
import mu.KotlinLogging
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

fun main() = ECommerceProducer.runProducer()

object ECommerceProducer {
    private val logger: KLogger by lazy { KotlinLogging.logger {} }

    fun runProducer() {
        val events: Sequence<ClickEvent> = DataSourceUtils
            .loadDataFile("events.csv", DataSourceUtils.toEvent)

        val properties = KafkaConfig.buildProducerProps(false)

        val producerResource: ProducerResource<String, ClickEvent> = ProducerResource.live<String, ClickEvent>(properties)

        val time = measureTimeMillis {
            for (event in events) {
                producerResource.produce(KafkaConfig.EVENTS_TOPIC, event.userid, event)
            }
        }
        producerResource.flush()
        logger.info("Total time '${TimeUnit.MILLISECONDS.toSeconds(time)}' seconds")

        producerResource.shutdown()
    }
}