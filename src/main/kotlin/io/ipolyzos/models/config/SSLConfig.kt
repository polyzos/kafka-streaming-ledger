package io.ipolyzos.models.config

data class SSLConfig(val truststoreLocation: String,
                     val truststorePassword: String,
                     val keystoreType: String,
                     val keystoreLocation: String,
                     val keystorePassword: String,
                     val keyPassword: String)