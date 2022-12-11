package io.ipolyzos.utils

import io.ipolyzos.models.ClickEvent
import java.io.File
import java.sql.Timestamp

object DataSourceUtils {
    fun <T> loadDataFile(filename: String, fn: (x: String) -> T, withHeader: Boolean = true): Sequence<T> {
        val lines = File(System.getProperty("user.home") + filename)
            .inputStream()
            .bufferedReader()
            .lineSequence()
            .map { it.replace("\"", "") }

        return if (withHeader) lines.drop(1).map(fn) else lines.map(fn)
    }

    val toEvent: (String) -> ClickEvent = { line: String ->
        val tokens = line.split(",")
        val timestamp = Timestamp.valueOf(tokens[0].replace(" UTC", ""))

        ClickEvent(
            eventTime    = timestamp.time,
            eventType    = tokens[1],
            productId    = tokens[2],
            categoryId   = tokens[3],
            categoryCode = tokens[4],
            brand        = tokens[5],
            price        = tokens[6].toDouble(),
            userid       = tokens[7],
            userSession = tokens[8]
        )
    }
}