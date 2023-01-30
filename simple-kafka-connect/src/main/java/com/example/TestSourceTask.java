package com.example;


import java.util.Properties;

public class TeskSourceTask extends SourceTask {
    // 커넥터 버전 리턴
    @Override
    public String version() {}

    // 태스크가 시작할 때 필요한 로직 작성
    @Override
    public void start(Map<String,String> props) {}

    // 소스 애플리케이션 또는 소스 파일로부터 데이터를 읽어오는 로직 작성
    @Override
    public List<SourceRecord> poll() {}

    // ㅌ태스크가 종료될 때 필요한 로직 작성
    @Override
    public void stop() {}
}
