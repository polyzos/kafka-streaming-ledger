package io.ipolyzos.consumers

import io.ipolyzos.resources.ConsumerResource
import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.models.ClickEvent
import io.ipolyzos.utils.LoggingUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.concurrent.atomic.AtomicInteger


fun main() {
    val properties = KafkaConfig.buildConsumerProps(
        "ecommerce.events.group"
    )

    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    //        properties[ConsumerConfig.CLIENT_ID_CONFIG] = "client-$id"
//        properties[ConsumerConfig.FETCH_MAX_BYTES_CONFIG] = 20000 //"client-$id"
//        properties[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 10000 //"client-$id"
//        properties[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = 500 //"client-$id"
//        properties[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = 1000 //"client-$id"
//        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false" //"client-$id"

    //        val listener = ConsumerRebalanceListenerImpl(consumer)

    val consumePartitionMsgCount = AtomicInteger(0)
    with(LoggingUtils()) {
        ConsumerResource<String, ClickEvent> (
            id = 1,
            topic = KafkaConfig.EVENTS_TOPIC,
            properties = properties,
            consumePartitionMsgCount = consumePartitionMsgCount,
            consumeFixed = true,
            numMessages = 1000000
        )
    }
}
