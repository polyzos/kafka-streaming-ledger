package io.ipolyzos.producers.interceptors

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.lang.Exception

class CustomInterceptor: ProducerInterceptor<String, String> {
    override fun configure(configs: MutableMap<String, *>?) {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        TODO("Not yet implemented")
    }

    override fun onSend(record: ProducerRecord<String, String>?): ProducerRecord<String, String> {
        TODO("Not yet implemented")
    }
}