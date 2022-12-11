package io.ipolyzos

import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.*

private val logger: KLogger by lazy { KotlinLogging.logger {} }

fun <K, V> ProducerRecord<K, V>.show(){
    // TODO:
}

fun <K, V> ConsumerRecords<K, V>.show() {
    this.first().offset()
    this.first().topic()
    this.first().partition()
    this.first().timestamp()
    this.first().key()
    this.first().value()
    this.first().timestampType()
    this.first().headers()
    this.first().leaderEpoch()

    logger.info { "Batch Contains: ${this.count()} records from ${this.map { it.partition() }.distinct().count()} partitions." }
    val data: MutableList<List<String>> = mutableListOf(
        listOf(
            "Offset",
            "Topic",
            "Partition",
            "Timestamp",
            "Key",
            "Value",
            "TimestampType",
            "Teaders",
            "LeaderEpoch",
        )
    )

    this.reversed().take(5) .forEach { record ->
        data.add(
            listOf(
                record.offset().toString(),
                record.topic().toString(),
                record.partition().toString(),
                record.timestamp().toString(),
                record.key().toString(),
                record.value().toString().take(40) + " ...",
                record.timestampType().toString(),
                record.headers().toString(),
                record.leaderEpoch().toString()
            )
        )
    }
    formatListAsTable(data)
}

fun RecordMetadata.show(){
    val data = listOf(
        listOf("Offset", "Topic", "Partition", "SerializedKeySize", "SerializedValueSize", "Timestamp"),
        listOf(
            this.offset(),
            this.topic(),
            this.partition(),
            this.serializedKeySize(),
            this.serializedValueSize(),
            this.timestamp()
        )
    )
    formatListAsTable(data)
}


fun Properties.show() {
    val data: MutableList<List<String>> = mutableListOf(listOf("Key", "Value"))
    this.forEach { k, v -> data.add(listOf(k.toString(), v.toString())) }
    formatListAsTable(data)
}

fun formatListAsTable(table: List<List<Any>>) {
    val colWidths = transpose(table)
        .map { it.map { cell -> cell.toString().length }.max() + 2 }

    // Format each row
    val rows: List<String> = table.map {
        it.zip(colWidths).joinToString(
            prefix = "|",
            postfix = "|",
            separator = "|"
        ) { (item, size) -> (" %-" + (size - 1) + "s").format(item) }
    }

    val separator: String = colWidths.joinToString(prefix = "+", postfix = "+", separator = "+") { "-".repeat(it) }
    val data = rows.drop(1).joinToString("\n")

    val tableHeader = "\n$separator\n${rows.first()}\n$separator"
    if (data.isEmpty()) {
        logger.info { tableHeader }
    } else {
        logger.info { "$tableHeader\n$data\n$separator" }
    }
}

fun <E> transpose(xs: List<List<E>>): List<List<E>> {
    fun <E> List<E>.head(): E = this.first()
    fun <E> List<E>.tail(): List<E> = this.takeLast(this.size - 1)
    fun <E> E.append(xs: List<E>): List<E> = listOf(this).plus(xs)

    xs.filter { it.isNotEmpty() }.let { ys ->
        return when (ys.isNotEmpty()) {
            true -> ys.map { it.head() }.append(transpose(ys.map { it.tail() }))
            else -> emptyList()
        }
    }
}