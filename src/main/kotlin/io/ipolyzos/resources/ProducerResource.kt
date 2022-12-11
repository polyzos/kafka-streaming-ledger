package io.ipolyzos.resources

import io.ipolyzos.show
import io.ipolyzos.utils.LoggingUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

context(LoggingUtils)
class ProducerResource<K, V> private constructor(private val producer: KafkaProducer<K, V>) {
    private val counter = AtomicInteger(0)
    companion object {
        fun <K, V> live(properties: Properties): ProducerResource<K, V> {
            with(LoggingUtils()) {
                logger.info("Starting Kafka Producers with configs ...")
                properties.show()
                val producer = KafkaProducer<K, V>(properties)
                return ProducerResource(producer)
            }
        }
    }

    fun produce(topic: String, key: K, value: V) {
        ProducerRecord(topic, key, value)
            .also { record ->
                producer.send(record) { _, exception ->
                    exception?.let {
                        logger.error { "Error while producing: $exception" }
                    } ?: kotlin.run {
                        counter.incrementAndGet()
                        if (counter.get() % 100000 == 0) {
                            logger.info { "Total messages sent so far ${counter.get()}." }
                        }
                    }
                }
            }
    }

    fun flush() = producer.flush()
    fun shutdown() {
        producer.flush()
        logger.info { "Total Event records sent: '${counter.get()}' " }
        logger.info("Closing Producers ...");
        producer.close()
    }
}