module com.example.simplekafkaproducer {

    requires org.slf4j;
    requires kafka.clients;
    requires kafka.streams;


    opens com.example.chapter3.producer.simplekafkaproducer to javafx.fxml;
    exports com.example.chapter3.producer.simplekafkaproducer;
    exports com.example.chapter3.consumer.simpleConsumer;
    exports com.example.chapter3.producer.kafkaProducerAsyncCallback;
    opens com.example.chapter3.producer.kafkaProducerAsyncCallback to javafx.fxml;
    exports com.example.chapter3.producer.kafkaProducerCustomPartitioner;
    opens com.example.chapter3.producer.kafkaProducerCustomPartitioner to javafx.fxml;
    opens com.example.chapter3.consumer.simpleConsumer to javafx.fxml;
}