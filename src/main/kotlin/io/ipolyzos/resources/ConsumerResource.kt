package io.ipolyzos.resources


import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import io.ipolyzos.show
import io.ipolyzos.utils.LoggingUtils
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.time.Duration

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureTimeMillis

context(LoggingUtils)
class ConsumerResource<K, V> (
    id: Int, topic: String,
    properties: Properties,
    consumerParallel: Boolean = false,
    private val consumePartitionMsgCount: AtomicInteger,
    private val autoCommit: Boolean = true,
    private val perMessageCommit: Boolean = false
) {
    private val counter = AtomicInteger(0)
    private val consumerName = "Consumer-$id"
    private val consumer: KafkaConsumer<K, V>

    init {
        logger.info("Starting Kafka '$consumerName' in group '${properties["group.id"]}' with configs ...")
        properties.show()
        consumer = KafkaConsumer<K, V>(properties)
        registerShutdownHook()
        if (consumerParallel) consumeParallel(topic) else consume(topic)
    }

    private fun consume(topic: String) {
        consumer.subscribe(listOf(topic))
        try {
            while (true) {
                runConsumerLoop()
            }
        } catch (e: WakeupException) {
            logger.info("[$consumerName] Received Wake up exception!")
        } catch (e: Exception) {
            logger.warn("[$consumerName] Unexpected exception: {}", e.message)
        } finally {
            consumer.close()
            logger.info("[$consumerName] The consumer is now gracefully closed.")
        }
    }

    private fun runConsumerLoop() {
        val records: ConsumerRecords<K, V> = consumer.poll(Duration.ofMillis(100))
        if (records.count() > 0) {
            records.forEach { record ->
                // simulate the consumers doing some work
                Thread.sleep(20)
                if (perMessageCommit) {
                    val commitEntry: Map<TopicPartition, OffsetAndMetadata> =
                        mapOf(TopicPartition(record.topic(), record.partition()) to OffsetAndMetadata(record.offset()))
                    consumer.commitAsync(commitEntry) { topicPartition, offsetMetadata ->
                        logger.info { "Committed: $topicPartition and $offsetMetadata" }
                    }
                }
            }
            records.show()
            counter.addAndGet(records.count())
            consumePartitionMsgCount.addAndGet(records.count())
            logger.info { "[$consumerName] Records in batch: ${records.count()} - Total Consumer Processed: ${counter.get()} - Total Group Processed: ${consumePartitionMsgCount.get()}" }
            if (!autoCommit && !perMessageCommit) {
                val maxOffset: Long = records.maxOfOrNull { it.offset() }!!
                commitMessage(maxOffset)
            }

        //                val commitEntry: Map<TopicPartition, OffsetAndMetadata> =
//                    mapOf(TopicPartition(r.topic(), r.partition()) to OffsetAndMetadata(r.offset()))
//                consumer.commitAsync(commitEntry) { topicPartition, offsetMetadata ->
//                    logger.info { "Committed: $topicPartition and $offsetMetadata" }
//                }
//                    consumer.commitSync(commitEntry)
//                    logger.info { "Committed: $commitEntry" }
        }
    }

    private fun commitMessage(maxOffset: Long) {
        consumer.commitAsync { topicPartition, offsetMetadata ->
            topicPartition.forEach { v -> println(v) }
            logger.info { "Processed up to offset: $maxOffset - Committed: $topicPartition and $offsetMetadata" }
        }
    }

    fun consumeParallel(topic: String) {
        val options: ParallelConsumerOptions<K, V> =
            ParallelConsumerOptions.builder<K, V>()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(100)
                .consumer(consumer)
                .batchSize(5)
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build()

        val eos = ParallelStreamProcessor.createEosStreamProcessor(options)
        eos.subscribe(listOf(topic))
        var initLoop = false
        var t0 = System.currentTimeMillis()

        eos.poll { context ->
            if (!initLoop) {
                initLoop = true
                t0 = System.currentTimeMillis()
            }

            val processingTime = measureTimeMillis {
                context.consumerRecordsFlattened.forEach { _ ->
                    Thread.sleep(20)
                }
            }
            counter.addAndGet(context.consumerRecordsFlattened.count())
            consumePartitionMsgCount.addAndGet(context.consumerRecordsFlattened.count())
            logger.info { "[$consumerName] Records in batch: ${context.consumerRecordsFlattened.count()} - Total Consumer Processed: ${counter.get()} - Total Group Processed: ${consumePartitionMsgCount.get()}" }
        }
    }

    private fun registerShutdownHook() {
        val currentThread = Thread.currentThread()

        Runtime.getRuntime().addShutdownHook(Thread {
            logger.info("[$consumerName] Detected a shutdown, let's exit by calling consumer.wakeup()...")
            consumer.wakeup()
            try {
                currentThread.join()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        })
    }
}
