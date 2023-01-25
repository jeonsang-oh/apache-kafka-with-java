package com.example.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;



/* 스트림 프로세서 클래스를 생성하기 위해선 kafka-streams 라이브러리에서 제공하는
*  Processor 또는 Transformer 인터페이스를 사용한다.*/
public class FilterProcessor implements Processor<String, String> {

    /* ProcessorContext 클래스는 프로세서에 대한 정보를 담고 있다.
    * 생성된 인스턴스로 현재 스트림 처리 중인 토폴로지의 토픽 정보, 애플리케이션 아이디를 조회할 수 있다. */
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        if (value.length() > 5) {
            context.forward(key, value);
        }
        context.commit();
    }

    @Override
    public void close() {

    }
}
