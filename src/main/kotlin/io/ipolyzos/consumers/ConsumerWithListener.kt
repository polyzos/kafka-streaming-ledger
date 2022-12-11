//package io.ipolyzos.consumers
//
//import io.ipolyzos.resources.ConsumerResource
//import io.ipolyzos.config.KafkaConfig
//import io.ipolyzos.models.ClickEvent
//import io.ipolyzos.utils.LoggingUtils
//import org.apache.kafka.clients.consumer.ConsumerConfig
//
//
//fun main() {
//    val properties = KafkaConfig.buildConsumerProps(
//        "ecommerce.events.group"
//    )
//
//    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
//    with(LoggingUtils()) {
//        ConsumerResource<String, ClickEvent>(1, KafkaConfig.EVENTS_TOPIC, properties)
//    }
//}
