package io.ipolyzos.utils

import mu.KLogger
import mu.KotlinLogging

interface LoggerService {
    val logger: KLogger
}
class LoggingUtils: LoggerService {
    override val logger: KLogger by lazy { KotlinLogging.logger {} }
}