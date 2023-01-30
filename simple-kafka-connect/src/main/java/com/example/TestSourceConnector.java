package com.example;


import java.util.Properties;

public class TestSourceConnector extends SourceConnector {
    // 커넥터 버전 리턴
    @Override
    public String version() {}

    // JSON 또는 config 파일 형태로 입력한 설정값을 초기화하는 메서드
    @Override
    public void start(Map<String,String> props) {}

    // 이 커넥터가 사용할 태스크 크래스를 지정한다.
    @Override
    public Class<? extends Task> taskClass() {}

    // 태스크 개수가 2개 이상인 경우 태스크마다 각기 다른 옵션을 설정할 때 사용한다.
    @Override
    public List<Map<String,String>> taskConfigs(int MaxTask) {}

    // 커넥터가 사용할 설정값에 대한 정보를 받는다.
    @Override
    public ConfigDef config() {}

    // 커넥터가 종료될 때 필요한 로직을 작성한다.
    @Override
    public void stop() {}
}
