package com.bnitech.elasticsearchkafkaconnector;

import com.bnitech.elasticsearchkafkaconnector.config.ElasticSearchSinkConnectorConfig;
import com.google.gson.Gson;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.bnitech.elasticsearchkafkaconnector.config.ElasticSearchSinkConnectorConfig.ES_CLUSTER_HOST;
import static com.bnitech.elasticsearchkafkaconnector.config.ElasticSearchSinkConnectorConfig.ES_CLUSTER_PORT;
import static com.bnitech.elasticsearchkafkaconnector.config.ElasticSearchSinkConnectorConfig.ES_INDEX;

public class ElasticSearchSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkTask.class);


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
                RestClient.builder(new HttpHost(config.getString(ES_CLUSTER_HOST),
                        config.getInt(ES_CLUSTER_PORT))));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() > 0) {
            BulkRequest bulkRequest = new BulkRequest();
            for (SinkRecord record : records) {
                Gson gson = new Gson();
                Map map = gson.fromJson(record.value().toString(), Map.class);
                bulkRequest.add(new IndexRequest(config.getString(ES_INDEX))
                        .source(map, XContentType.JSON));
                logger.info("record : {}", record.value());
            }

            esClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures()) {
                        logger.error(bulkResponse.buildFailureMessage());
                    } else {
                        logger.info("bulk save success");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.info("flush");
    }

    @Override
    public void stop() {
        try {
            esClient.close();
        } catch (IOException e) {
            logger.info(e.getMessage(), e);
        }
    }
}
