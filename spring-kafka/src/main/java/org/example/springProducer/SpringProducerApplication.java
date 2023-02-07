package org.example.springProducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

// 기존 KafkaTemplate 사용 클래스
@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "test";

    // KafkaTemplate을 @Autowired 어노테이션으로 주입받아서 사용한다.
    // 사용자가 직접 선언하지 않은 bean객체지만 스프링 카프카에서 제공하는 기본 KafkaTemplate 객체로 주입된다.
    // application.yaml에 선언한 옵션값은 자동으로 주입된다.
    @Autowired
    private KafkaTemplate<Integer, String> template;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    @Override
    public void run(String... args) {
        for (int i = 0; i < 10; i++) {
            // send() 메서드를 사용해 토픽 이름과 메시지 값을 넣어 전송한다.
            template.send(TOPIC_NAME, "test" + i);
        }
        // 프로듀서를 데이터 전송이 완료되면 종료한다.
        System.exit(0);
    }
}
