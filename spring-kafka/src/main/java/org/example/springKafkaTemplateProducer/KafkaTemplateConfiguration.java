package org.example.springKafkaTemplateProducer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

// KafkaTemplate 빈 객체 등록을 위해 Configuration 어노테이션을 선언한 클래스를 만든다.
// 이 클래스에서 KafkaTemplate 빈 객체가 등록된다.
@Configuration
public class KafkaTemplateConfiguration {

    // KafkaTemplate 객체를 리턴하는 빈 객체.
    @Bean
    public KafkaTemplate<String, String> customKafkaTemplate() {

        // ProducerFactory를 사용하여 KafkaTemplate 객체를 만들 때는 프로듀서 옵션을 직접 넣는다.
        // 카프카 기본 템플릿과 다르게 직접 프로듀서 옵션들을 선언해야 한다.
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // KafkaTemplate 객체를 만들기 위한 ProducerFactory를 초기화한다.
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);

        // 빈 객체로 사용할 KafkaTemplate 인스턴스를 초기화하고 리턴한다.
        return new KafkaTemplate<>(pf);
    }

}
