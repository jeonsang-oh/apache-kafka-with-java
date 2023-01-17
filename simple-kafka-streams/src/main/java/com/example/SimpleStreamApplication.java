package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApplication {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {
        Properties props = new Properties();

        /* 애플리케이션 아이디 지정, 애플리케이션 아이디 값 기준으로 병렬 처리 됨 */
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        /* 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보 */
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        /* 스트림 처리를 위해 메시지 키,값의 역직렬화, 직렬화 방식 지정, 데이터 처리 시 키, 값을 역직렬화 사용하고 최종적으로 토픽에 넣을 시 직렬화 */
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        /* 스트림 토폴로지를 정의하기 위한 용도 */
        StreamsBuilder builder = new StreamsBuilder();
        /* stream_log 토픽으로부터 KStream 객체를 만들기 위해 StreamBuilder의 stream() 메서드 사용 */
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);
        /* stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송키 위해 to() 메서드 사용
        * to() 메서드는 KStream 인스턴스의 데이터들을 특정 토픽으로 저장하기 위한 용도로 사용된다.
        * 즉, to() 메서드는 싱크 프로세서이다.*/
        streamLog.to(STREAM_LOG_COPY);

        /* StreamBuilder로 정의한 파라미터로 인스턴스 생성
        * 인스턴스 실행하려면 start() 메서드 사용
        * 이 스트림즈 애플리케이션은 stream_log 토픽의 데이터를 stream_log_copy 토픽으로 전달한다. */
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
