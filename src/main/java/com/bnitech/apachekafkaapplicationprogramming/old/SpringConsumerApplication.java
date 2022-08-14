package com.bnitech.apachekafkaapplicationprogramming.old;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class SpringConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringConsumerApplication.class, args);
    }

    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void batchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> log.info(record.toString()));
    }

    @KafkaListener(topics = "test", groupId = "test-group-02")
    public void singleTopicListener(List<String> list) {
        list.forEach(log::info);
    }

    @KafkaListener(topics = "test", groupId = "tset-group-03", concurrency = "3")
    public void listenSpecificPartition(ConsumerRecords<String, String> records) {
        records.forEach(record -> log.info(record.toString()));
    }
}
