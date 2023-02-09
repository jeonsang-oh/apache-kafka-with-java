
package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

@SpringBootApplication
public class SpringConsumerApplication {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }

    // 컨슈머 레코드의 묶음(ConsumerRecords)을 파라미터로 받는다.
    // 카프카 클라이언트 라이브러리에서 poll() 메서드로 리턴받은 ConsumerRecords를 리턴받아 사용하는 것과 동일하다.
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void batchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> logger.info(record.toString()));
    }

    // 메시지 값들을 List 자료구조로 받아서 처리한다.
    @KafkaListener(topics = "test", groupId = "test-group-02")
    public void batchListener(List<String> list) {
        list.forEach(recordValue -> logger.info(recordValue));
    }

    // 2개 이상의 컨슈머 스레드로 배치 리스너를 운영할 경우 concurrency 옵션을 함께 선언하용 사용하면 된다.
    // 3개의 컨슈머 스레드가 생성된다.
    @KafkaListener(topics = "test", groupId = "test-group-03", concurrency = "3")
    public void concurrentBatchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> logger.info(record.toString()));
    }

}
