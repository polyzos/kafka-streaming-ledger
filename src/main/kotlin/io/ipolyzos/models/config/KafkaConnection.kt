package io.ipolyzos.models.config

data class KafkaConnection (
    val servers: String,
    val securityProtocol: String,
    val ssl: SSLConfig
)