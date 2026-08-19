package com.osscontest.worker.indexing.retrieval.config

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import java.net.URI

@Configuration
@EnableConfigurationProperties(StorageProperties::class)
class S3ClientConfiguration {
    @Bean
    fun s3Client(properties: StorageProperties): S3Client {
        val builder =
            S3Client.builder()
                .region(Region.of(properties.region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .serviceConfiguration(
                    S3Configuration.builder().pathStyleAccessEnabled(true).build(),
                )
                .overrideConfiguration(
                    ClientOverrideConfiguration.builder()
                        .apiCallTimeout(properties.downloadTimeout)
                        .build(),
                )
        properties.endpoint?.takeIf { it.isNotBlank() }?.let { builder.endpointOverride(URI.create(it)) }
        return builder.build()
    }
}
