package com.bnitech.apachekafkaapplicationprogramming;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
@RequiredArgsConstructor
public class ApacheKafkaApplicationProgrammingApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "test";

    private final KafkaTemplate<Integer, String> template;

    public static void main(String[] args) {
        SpringApplication.run(ApacheKafkaApplicationProgrammingApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; i < 10; i++) {
            template.send(TOPIC_NAME, "test" + i);
        }
        System.exit(0);
    }
}
