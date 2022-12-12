package io.ipolyzos.consumers.offsets

import io.ipolyzos.resources.ConsumerResource
import io.ipolyzos.config.KafkaConfig
import io.ipolyzos.utils.LoggingUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.concurrent.atomic.AtomicInteger


fun main() {
    val properties = KafkaConfig.buildConsumerProps(
        "test.group.batch"
    )
    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"

    val consumePartitionMsgCount = AtomicInteger(0)
    with(LoggingUtils()) {
        ConsumerResource<String, String> (
            id = 1,
            topic = KafkaConfig.TEST_TOPIC,
            properties = properties,
            consumerParallel = false,
            consumePartitionMsgCount = consumePartitionMsgCount,
            autoCommit=false,
            perMessageCommit=false
        )
    }
}
