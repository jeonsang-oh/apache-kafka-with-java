package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class SpringConsumerApplication {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }

    // KafkaListener 어노테이션의 containerFactory 옵션을 커스텀 컨테이너 팩토리로 설정한다.
    // 빈 객체로 등록한 이름인 customContainerFactory를 옵션값으로 설정하면 커스텀 컨테이너 팩토리로 생성된 커스텀 리스너 컨테이너를 사용할 수 있다.
    @KafkaListener(topics = "test",
            groupId = "test-group",
            containerFactory = "customContainerFactory")
    public void customListener(String data) {
        logger.info(data);
    }
}

