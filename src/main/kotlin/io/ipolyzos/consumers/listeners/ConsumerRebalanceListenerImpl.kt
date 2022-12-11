package io.ipolyzos.consumers.listeners

import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

class ConsumerRebalanceListenerImpl<K, V>(private val consumer: KafkaConsumer<K, V>): ConsumerRebalanceListener {
    private val logger: KLogger by lazy { KotlinLogging.logger {} }

    private val currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata> = mutableMapOf()

    fun addOffsetToTrack(topic: String?, partition: Int, offset: Long) {
        currentOffsets[TopicPartition(topic, partition)] = OffsetAndMetadata(offset + 1, null)
    }
    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        logger.info("onPartitionsRevoked callback triggered");
        logger.info("Committing offsets: $currentOffsets");

        consumer.commitSync(currentOffsets);
    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
        logger.info("onPartitionsAssigned callback triggered");
    }

    // this is used when we shut down our consumer gracefully
    fun getCurrentOffsets(): Map<TopicPartition, OffsetAndMetadata> {
        return currentOffsets
    }
}