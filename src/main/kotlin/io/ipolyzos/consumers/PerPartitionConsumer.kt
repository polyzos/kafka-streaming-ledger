package io.ipolyzos.consumers

import io.ipolyzos.resources.ConsumerResource
import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.models.ClickEvent
import io.ipolyzos.utils.LoggingUtils
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.concurrent.atomic.AtomicInteger


fun main() = runBlocking {
    val properties = KafkaConfig.buildConsumerProps(
        "ecommerce.events.group"
    )

    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    val adminClient = AdminClient.create(properties)

    val partitionNum = adminClient
        .describeTopics(listOf(KafkaConfig.EVENTS_TOPIC))
        .topicNameValues()[KafkaConfig.EVENTS_TOPIC]
        ?.get()
        ?.partitions()
        ?.size!!

    val consumePartitionMsgCount = AtomicInteger(0)
    for (id in 0..partitionNum) {
        with(LoggingUtils()) {
            launch(Dispatchers.IO) {
                ConsumerResource<String, ClickEvent> (
                    id = id,
                    topic = KafkaConfig.EVENTS_TOPIC,
                    properties = properties,
                    consumePartitionMsgCount = consumePartitionMsgCount
                )
            }
        }
    }
}