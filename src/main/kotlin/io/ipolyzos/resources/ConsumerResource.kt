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

    private var initLoop = false
    private var t0 = System.currentTimeMillis()
    private var lastOffsetCommitted: Long = -1

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
        if (!initLoop) {
            initLoop = true
            t0 = System.currentTimeMillis()
        }
        val records: ConsumerRecords<K, V> = consumer.poll(Duration.ofMillis(100))
        if (records.count() > 0) {
            records.forEach { record ->
                // simulate the consumers doing some work
                Thread.sleep(20)
                if (perMessageCommit) {
                    logger.info { "Processed Message: ${record.value()} with offset: ${record.partition()}:${record.offset()}" }
                    val commitEntry: Map<TopicPartition, OffsetAndMetadata> =
                        mapOf(TopicPartition(record.topic(), record.partition()) to OffsetAndMetadata(record.offset()))

                    consumer.commitSync(commitEntry)
                    logger.info { "Committed offset: ${record.partition()}:${record.offset()}" }
                }
            }
            records.show()
            counter.addAndGet(records.count())
            consumePartitionMsgCount.addAndGet(records.count())
            logger.info { "[$consumerName] Records in batch: ${records.count()} - Elapsed Time: ${TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()-t0)} seconds - Total Consumer Processed: ${counter.get()} - Total Group Processed: ${consumePartitionMsgCount.get()}" }
            if (!autoCommit && !perMessageCommit) {
                val maxOffset: Long = records.maxOfOrNull { it.offset() }!!
                commitOffsets(maxOffset)
            }
        }
    }

    private fun commitOffsets(maxOffset: Long) {
        consumer.commitSync()
        logger.info { "Previous last offset: $lastOffsetCommitted - Committed offset: $maxOffset" }
        lastOffsetCommitted = maxOffset
//        { topicPartition, offsetMetadata ->

//            logger.info { "Processed up to offset: $maxOffset - Committed: $topicPartition and $offsetMetadata" }
//        }
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

        eos.poll { context ->
            if (!initLoop) {
                initLoop = true
                t0 = System.currentTimeMillis()
            }

            context.consumerRecordsFlattened.forEach { _ ->
                Thread.sleep(20)
            }

            counter.addAndGet(context.consumerRecordsFlattened.count())
            consumePartitionMsgCount.addAndGet(context.consumerRecordsFlattened.count())
            logger.info {
                "[$consumerName] Records in batch: ${context.consumerRecordsFlattened.count()} - Elapsed Time: ${
                    TimeUnit.MILLISECONDS.toSeconds(
                        System.currentTimeMillis() - t0
                    )
                } seconds - Total Consumer Processed: ${counter.get()} - Total Group Processed: ${consumePartitionMsgCount.get()}"
            }
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
