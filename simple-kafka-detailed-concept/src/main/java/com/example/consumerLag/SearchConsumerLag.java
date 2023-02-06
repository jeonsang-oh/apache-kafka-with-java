package com.example.consumerLag;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;

public class SearchConsumerLag {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    Properties configs = new Properties();

    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

    public static void main(String[] args) {

        for (Map.Entry<MetricName, ? extends Metric> entry : kafkaConsumer.metrics().entrySet()) {
            if ("records-lag-max".equals(entry.getKey().name()) |
            "records-lag".equals(entry.getKey().name()) |
            "records-lag-avg".equals(entry.getKey().name())) {
                Metric metric = entry.getValue();
                logger.info("{}:{}", entry.getKey().name(), metric.metricValue());
            }
        }
    }
}
