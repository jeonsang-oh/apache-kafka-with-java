package com.pipeline.favoriteColorWebpage.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// 컨슈머가 실행될 스레드를 정의하기 위해 Runnable 인터페이스로 ConsumerWorker클래스를 구현하였다.
// ConsumerWorker 클래스를 스레드로 실행하면 오버라이드된 run() 메서드가 호출된다.
public class ConsumerWorker implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    // 컨슈머의 poll() 메서드를 통해 전달받은 데이터를 임시 저장하는 버퍼를 bufferString으로 선언했다.
    // static 변수로 선언하여 다수의 스레드가 만들어지더라도 동일 변수에 접근한다.
    // 다수 스레드가 동시 접근할 수 있는 변수이므로 멀티 스레드 환경에서 안전하게 사용할 수 있는 ConcurrentHashMap<>으로 구현했다.
    // bufferString에는 파티션번호와 메시지 값들이 들어간다.
    // currentFileOffset은 오프셋 값을 저장하고 파일 이름을 저장할 때 오프셋 번호를 붙이는 데에 사용된다.
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();
    private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>();

    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties prop, String topic, int number) {
        logger.info("Generate ConsumerWorker");
        this.prop = prop;
        this.topic = topic;
        // 스레드에 이름을 붙임으로써 로깅 시에 편리하게 스레드 번호를 확인할 수 있다.
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        // 스레드를 생성하는 HdfsSinkApllication에서 설정한 컨슈머 설정을 가져와서 KafkaConsumer 인스턴스를 생성하고 토픽을 구독한다.
        consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                // poll() 메서드로 가져온 데이터들을 버퍼에 쌓기 위해 addHdfsFileBuffer() 메서드를 호출한다.
                for (ConsumerRecord<String, String> record : records) {
                    addHdfsFileBuffer(record);
                }
                // 한번 polling이 완료도고 버퍼에 쌓이고 나면 saveBufferToHdfsFile()을 호출하여
                // 버퍼에 쌓인 데이터가 일정 개수 이상 쌓였을 경우 HDFS에 저장하는 로직을 수행하도록 한다.
                // 여기서 consumer.assignment() 값을 파라미터로 넘겨주는데,
                // 현재 컨슈머 스레드에 할당된 파티션에 대한 버퍼 데이터만 적재할 수 있도록 하기 위함이다.
                saveBufferToHdfsFile(consumer.assignment());
            }
        } catch (WakeupException e) {
            // 안전한 종료를 위해 WakeUpException을 받아서 컨슈머 종료 과정을 수행한다.
            logger.warn("Wakeup consumer");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    // 레코드를 받아서 메시지 값을 버퍼에 넣는다.
    private void addHdfsFileBuffer(ConsumerRecord<String, String> record) {
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        // 버퍼 크기가 1이라면 버퍼의 가장 처음 오프셋을 뜻하므로 currentFileOffset 변수에 넣는다.
        // currentFileOffset 변수에서 번호를 관리하면 추후 파일 저장 시 파티션 이름과 오프셋 번호를 붙여서 저장할 수 있기 때문에
        // 이슈 발생 시 파티션과 오프셋에 대한 정보를 알 수 있다는 장점이 있다.
        if (buffer.size() == 1)
            currentFileOffset.put(record.partition(), record.offset());
    }

    // 버퍼의 데이터가 flush될 만큼 개수가 충족되었는지 확인하는 checkFlushCount 메서드를 호출한다.
    // 컨슈머로부터 Set<TopicPartition> 정보를 받아서 컨슈머 스레드에 할당된 파티션에만 접근한다.
    private void saveBufferToHdfsFile(Set<TopicPartition> partitions) {
        partitions.forEach(p -> checkFlushCount(p.partition()));
    }

    // 파티션 번호의 버퍼를 확인하여 flush를 수행할 만큼 레코드 개수가 찼는지 확인한다.
    // 만약 일정 개수 이상이면 HDFS 적재 로직인 save() 메서드를 호출한다.
    // 여기서는 FLUSH_RECORD_COUNT를 10으로 설정했기 때문에 버퍼에 10개 이상의 데이터가 쌓이면 save()가 호출된다.
    private void checkFlushCount(int partitionNo) {
        if (bufferString.get(partitionNo) != null) {
            if (bufferString.get(partitionNo).size() > FLUSH_RECORD_COUNT - 1) {
                save(partitionNo);
            }
        }
    }

    // 실질적인 HDFS 적재를 수행하는 메서드.
    private void save(int partitionNo) {
        if (bufferString.get(partitionNo).size() > 0)
            try {
                // HDFS에 저장할 파일이름.
                String fileName = "/data/color-" + partitionNo + "-" + currentFileOffset.get(partitionNo) + ".log";
                // 적재를 위한 설정 수행.
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem hdfsFileSystem = FileSystem.get(configuration);
                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName));
                fileOutputStream.writeBytes(StringUtils.join(bufferString.get(partitionNo), "\n"));
                fileOutputStream.close();

                // 버퍼 데이터가 적재 완료되었다면 새로 ArrayList를 선언하여 버퍼 데이터를 초기화 한다.
                bufferString.put(partitionNo, new ArrayList<>());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
    }

    // 버퍼에 남아있는 모든 데이터를 저장하기 위한 메서드.
    private void saveRemainBufferToHdfsFile() {
        bufferString.forEach((partitionNo, v) -> this.save(partitionNo));
    }

    public void stopAndWakeup() {
        logger.info("stopAndWakeup");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }
}