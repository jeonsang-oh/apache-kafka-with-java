package com.pipeline.favoriteColorWebpage.producer;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
// REST-API를 다른 도메인에서도 호출할 수 있도록 CORS 설정을 위해 어노테이션 추가
@CrossOrigin(origins = "*", allowedHeaders = "*")
public class ProduceController {
    private final Logger logger = LoggerFactory.getLogger(ProduceController.class);
    // 카프카 스프링의 KafkaTemplate 인스턴스를 생성한다.
    // 메시지 키와 메시지 값은 String 타입으로 설정한다.
    private final KafkaTemplate<String, String> kafkaTemplate;

    public ProduceController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // GET 메서드 호출을 받기 위한 어노테이션
    // GET 메서드 호출로 color와 user 파라미터를 추가로 받아 변수에 넣는다.
    // 브라우저에서 호출 시 자동으로 들어가는 헤더 값인 user-agent도 userAgentName 변수로 받는다.
    @GetMapping("/api/select")
    public void selectColor(
            @RequestHeader("user-agent") String userAgentName,
            @RequestParam(value = "color") String colorName,
            @RequestParam(value = "user") String userName) {

        // 최종 적재되는 데이터에 시간 저장을 위해 String 타입으로 변환한 시간을 저장한다.
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss.SSSZZ");
        Date now = new Date();
        // Gson은 구글에서 만든 라이브러리로써 자바 객체를 JSON포맷의 String 타입으로 변환할 수 있다.
        Gson gson = new Gson();
        UserEventVO userEventVO = new UserEventVO(sdfDate.format(now), userAgentName, colorName, userName);
        // Gson을 통해 UserEventVO 인스턴스를 JSON포맷의 String 타입으로 변환한다.
        String jsonColorLog = gson.toJson(userEventVO);

        // select-color 토픽에 데이터를 전송한다.
        // 메시지 키를 지정하지 않으므로 send() 메서드에는 토픽과 메시지 값만 넣으면 된다.
        // 추가로 데이터가 정상적으로 전송되었는지 확인하기 위해 addCallback() 메서드를 붙인다.
        kafkaTemplate.send("select-color", jsonColorLog).addCallback(
                new ListenableFutureCallback<SendResult<String, String>>() {

                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        logger.info(result.toString());
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        logger.error(ex.getMessage(), ex);
                    }
                });
    }
}
