package org.example.springKafkaTemplateProducer;

import org.example.springProducer.SpringProducerApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootApplication
public class springProducerApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "my-kafka:9092";

    // 빈 객체로 등록한 customKafkaTemplate을 주입받도록 메서드 이름과 동일한 변수명 선언.
    @Autowired
    private KafkaTemplate<String, String> customKafkaTemplate;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    @Override
    public void run(String... args) {
        // 커스텀 카프카 템플릿을 주입받은 이후에 KafkaTemplate 인스턴스를 사용하는 것은
        // 기본 카프카 템플릿을 사용하는 것과 동일하다.
        // send() 메서드를 사용하여 특정 토픽으로 데이터를 전송할 수 있다.
        // 만약 전송한 이후에 정상 적재됐는지 여부를 확인하고 싶다면 ListenableFuture 메서드를 사용하면 된다.
        ListenableFuture<SendResult<String, String>> future = customKafkaTemplate.send(TOPIC_NAME, "test");
        // ListenableFutrue 인스턴스에 addCallback 함수를 붙여 프로듀서가 보낸 데이터의 브로커 적재여부를 비동기로 확인할 수 있다.
        // 만약 브로커에 정상 적재되었다면 onSuccess메서드가 호출된다.
        // 적재되지 않고 이슈가 발생했다면 OnFailure 메서드가 호출된다.
        future.addCallback(new KafkaSendCallback<String, String>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
            }

            @Override
            public void onFailure(KafkaProducerException ex) {
            }
        });
        System.exit(0);
    }
}
