package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

@SpringBootApplication
public class SpringConsumerApplication {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);


    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }

    // AckMode를 MANUAL 또는 MANUAL_IMMEDIATE로 사용할 경우 수동커밋을 하기 위해 파라미터로 Acknowledgment를 받아야 한다.
    // acknowledge() 메서드를 호출함으로써 커밋을 수행할 수 있다.
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void commitListener(ConsumerRecords<String, String> records, Acknowledgment ack) {
        records.forEach(record -> logger.info(record.toString()));
        ack.acknowledge();
    }

    // 동기 커밋, 비동기 커밋을 사용하고 싶다면 컨슈머 인스턴스를 파라미터로 받아 사용할 수 있다.
    // consumer 인스턴스의 commitSync(), commitAsync() 메서드를 호출하면
    // 사용자가 원하는 타이밍에 커밋할 수 있도록 로직을 추가할 수 있다.
    // 다만, 리스너가 커밋을 하지 않도록 AckMode는 MANUAL 또는 MANUAL_IMMEDIATE로 설정해야한다.
    @KafkaListener(topics = "test", groupId = "test-group-02")
    public void consumerCommitListener(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
        records.forEach(record -> logger.info(record.toString()));
        consumer.commitSync();
    }
}