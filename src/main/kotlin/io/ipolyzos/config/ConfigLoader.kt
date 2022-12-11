package io.ipolyzos.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource
import com.sksamuel.hoplite.yaml.YamlParser
import io.ipolyzos.models.config.ConnectionsConfig

class ConfigLoader private constructor(){
    companion object Factory {
        fun loadConfig(fileName: String = "/application.yaml"): ConnectionsConfig = ConfigLoaderBuilder
            .default()
            .addParser("yaml", YamlParser())
            .addResourceSource(fileName)
            .build()
            .loadConfigOrThrow<ConnectionsConfig>()
    }
}