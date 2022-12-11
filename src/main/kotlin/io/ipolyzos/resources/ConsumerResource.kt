package io.ipolyzos.resources


import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import io.ipolyzos.show
import io.ipolyzos.utils.LoggingUtils
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
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
    private val consumePartitionMsgCount: AtomicInteger,
    private val consumeFixed: Boolean = false,
    private val numMessages: Int = -1,
    consumerParallel: Boolean = false
) {
    private val counter = AtomicInteger(0)
    private val consumerName = "Consumer-$id"
    private val consumer: KafkaConsumer<K, V>

    init {
        logger.info("Starting Kafka '$consumerName' in group '${properties["group.id"]}' with configs ...")
        properties.show()
        consumer = KafkaConsumer<K, V>(properties)
        val mainThread = Thread.currentThread()

        Runtime.getRuntime().addShutdownHook(Thread {
            logger.info("[$consumerName] Detected a shutdown, let's exit by calling consumer.wakeup()...")
            consumer.wakeup()

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        })
        if (consumerParallel) consumeParallel(topic) else consume(topic)
    }

    private fun consumeInfinite() {
        while (true) {
            runConsumerLoop()
        }
    }

    private fun consumeFixed(numMessages: Int) {
        while (consumePartitionMsgCount.get() < numMessages) {
            runConsumerLoop()
        }
        logger.info { "[$consumerName] Finished Consuming $numMessages messages ..." }
    }

    private fun consume(topic: String) {
        consumer.subscribe(listOf(topic))
        try {
            when (consumeFixed) {
                true -> consumeFixed(numMessages)
                else -> {
                    consumeInfinite()
                }
            }
        } catch (e: WakeupException) {
            logger.info("[$consumerName] Received Wake up exception!")
        } catch (e: Exception) {
            logger.warn("[$consumerName] Unexpected exception: {}", e.message)
        } finally {

//            logger.info { "${listener.getCurrentOffsets()}" }
//            consumer.commitSync(listener.getCurrentOffsets())
            consumer.close() // this will also commit the offsets if need be.
            logger.info("[$consumerName] The consumer is now gracefully closed.")
        }
    }


    private fun runConsumerLoop() {
        val records: ConsumerRecords<K, V> = consumer.poll(Duration.ofMillis(100))
        if (records.count() > 0) {
            records.forEach { _ ->
                // simulate the consumers doing some work
//                Thread.sleep(20)
            }
            records.show()
            counter.addAndGet(records.count())
            consumePartitionMsgCount.addAndGet(records.count())
            logger.info { "[$consumerName] Records in batch: ${records.count()} - Total so far: ${counter.get()} - Topic: ${consumePartitionMsgCount.get()}" }

//                val commitEntry: Map<TopicPartition, OffsetAndMetadata> =
//                    mapOf(TopicPartition(r.topic(), r.partition()) to OffsetAndMetadata(r.offset()))
//                consumer.commitAsync(commitEntry) { topicPartition, offsetMetadata ->
//                    logger.info { "Committed: $topicPartition and $offsetMetadata" }
//                }
//                    consumer.commitSync(commitEntry)
//                    logger.info { "Committed: $commitEntry" }
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

            if (consumePartitionMsgCount.get() == numMessages) {
                logger.info { "Consumed $consumePartitionMsgCount messages ..." }
                logger.info { "[$consumerName] Elapsed Time: ${TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - t0)} seconds." }
                consumer.close()
            }

            val processingTime = measureTimeMillis {
                context.consumerRecordsFlattened.forEach { _ ->
                    Thread.sleep(20)
                }
            }
            counter.addAndGet(context.consumerRecordsFlattened.count())
            consumePartitionMsgCount.addAndGet(context.consumerRecordsFlattened.count())
            logger.info { "[$consumerName] Processing '${context.size()}' records in $processingTime millis. Total: ${counter.get()} - ${consumePartitionMsgCount.get()}" }
        }
    }
}
