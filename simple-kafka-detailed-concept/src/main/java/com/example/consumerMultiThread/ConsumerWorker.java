package com.example.consumerMultiThread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWorker implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    // KafkaConsumer 인스턴스를 생성하기 위해 필요한 변수를 컨슈머 스레드 생성자 변수로 받는다.
    // 카프카 컨슈머 옵션을 담는 Properties 클래스와 토픽이름, 스레드 번호를 변수로 받았다.
    ConsumerWorker(Properties prop, String topic, int number) {
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        // KafkaConsumer 클래스는 스레드 세이프하지 않다.
        // 스레드별로 KafkaConsumer 인스턴스를 별개로 만들어야 한다.
        consumer = new KafkaConsumer<>(prop);
        // 생성자 변수로 받은 토픽을 명시적으로 구독하기 시작한다.
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            // poll() 메서드를 통해 리턴받은 레코드들을 처리한다.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
        }
    }
}
