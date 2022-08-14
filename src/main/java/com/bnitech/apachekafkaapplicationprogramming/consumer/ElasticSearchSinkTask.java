package com.bnitech.apachekafkaapplicationprogramming.consumer;

import com.bnitech.apachekafkaapplicationprogramming.consumer.config.ElasticSearchSinkConnectorConfig;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.bnitech.apachekafkaapplicationprogramming.consumer.config.ElasticSearchSinkConnectorConfig.ES_CLUSTER_HOST;
import static com.bnitech.apachekafkaapplicationprogramming.consumer.config.ElasticSearchSinkConnectorConfig.ES_CLUSTER_PORT;
import static com.bnitech.apachekafkaapplicationprogramming.consumer.config.ElasticSearchSinkConnectorConfig.ES_INDEX;

@Slf4j
public class ElasticSearchSinkTask extends SinkTask {

    private ElasticSearchSinkConnectorConfig config;
    private RestHighLevelClient esClient;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            config = new ElasticSearchSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage(), e);
        }

        esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(config.getString(ES_CLUSTER_HOST), config.getInt(ES_CLUSTER_PORT)))
        );
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() > 0) {
            BulkRequest bulkRequest = new BulkRequest();
            for (SinkRecord record : records) {
                bulkRequest.add(
                        new IndexRequest(config.getString(ES_INDEX)).source(
                                new Gson().fromJson(record.value().toString(), Map.class),
                                XContentType.JSON
                        )
                );
                log.info("record : {}", record.value());
            }

            esClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures()) {
                        log.error(bulkResponse.buildFailureMessage());
                    } else {
                        log.info("bulk save success");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.info("flush");
    }

    @Override
    public void stop() {
        try {
            esClient.close();
        } catch (IOException e) {
            log.info(e.getMessage(), e);
        }
    }
}
