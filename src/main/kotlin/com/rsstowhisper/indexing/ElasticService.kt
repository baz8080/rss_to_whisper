package com.rsstowhisper.indexing

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.Header
import org.apache.http.HttpHost
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.http.ssl.SSLContextBuilder
import org.elasticsearch.client.RestClient
import org.slf4j.LoggerFactory

class ElasticService(host: String, apiKey: String?) {
    private val logger = LoggerFactory.getLogger(ElasticService::class.java)
    private val client: ElasticsearchClient

    init {
        val httpHost = HttpHost.create(host)

        val sslContext =
            SSLContextBuilder
                .create()
                .loadTrustMaterial(null) { _, _ -> true }
                .build()

        val headers = mutableListOf<Header>()
        if (!apiKey.isNullOrBlank()) {
            headers.add(BasicHeader("Authorization", "ApiKey $apiKey"))
        }

        val restClient =
            RestClient
                .builder(httpHost)
                .setDefaultHeaders(headers.toTypedArray())
                .setHttpClientConfigCallback { builder: HttpAsyncClientBuilder ->
                    builder
                        .setSSLContext(sslContext)
                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                }.build()

        val transport = RestClientTransport(restClient, JacksonJsonpMapper())
        client = ElasticsearchClient(transport)
    }

    fun dropAndRecreateIndex(indexName: String) {
        try {
            client.indices().delete { it.index(indexName) }
        } catch (e: Exception) {
            logger.warn("Could not delete index $indexName: ${e.message}")
        }

        client.indices().create { builder ->
            builder
                .index(indexName)
                .settings { s ->
                    s.otherSettings("index.store.preload", co.elastic.clients.json.JsonData.of(listOf("nvd", "dvd")))
                }
        }

        client.cluster().putSettings { builder ->
            builder.persistent("search.max_async_search_response_size", co.elastic.clients.json.JsonData.of("101mb"))
        }
    }

    fun bulkIndex(episodes: List<Map<String, Any?>>) {
        if (episodes.isEmpty()) return

        for (chunk in episodes.chunked(100)) {
            logger.info("Processing chunk of size ${chunk.size}")

            val bulkRequest =
                BulkRequest.Builder().also { builder ->
                    for (episode in chunk) {
                        val id = episode["_id"] as? String
                        val index = episode["_index"] as? String ?: "podcasts"

                        val doc = episode.filterKeys { it != "_id" && it != "_index" }

                        builder.operations { op ->
                            op.index(
                                IndexOperation.Builder<Map<String, Any?>>()
                                    .index(index)
                                    .id(id)
                                    .document(doc)
                                    .build(),
                            )
                        }
                    }
                }.build()

            val response = client.bulk(bulkRequest)
            if (response.errors()) {
                response.items().filter { it.error() != null }.forEach { item ->
                    logger.error("Bulk index error: ${item.error()?.reason()}")
                }
            }
        }
    }
}
