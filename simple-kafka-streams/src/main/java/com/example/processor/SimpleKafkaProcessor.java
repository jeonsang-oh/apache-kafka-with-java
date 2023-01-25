package com.example.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor {

    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Topology 클래스는 프로세서 API를 사용한 토폴로지를 구성하기 위해 사용된다.
        Topology topology = new Topology();
        // Stream_log 토픽을 소스 프로세서로 가져오기 위해 addSource() 메서드를 사용했다.
        topology.addSource("Source", STREAM_LOG)
                // 스트림 프로세서를 사용하기 위해 addProcessor() 메서드를 사용했다.
                // 부모 노드는 Source, 다음 프로세서는 Process 스트림 프로세스이다.
                .addProcessor("Process",
                        () -> new FilterProcessor(),"Source")
                // stream_log_filter를 싱크 프로세서로 사용하여 데이터를 저장하기 위해 addSink() 메서드를 사용했다.
                // 부모 노드는 Process 이다.
                .addSink("Sink",STREAM_LOG_FILTER,
                        "Process");

        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();
    }
}
