module com.example.simplekafkaproducer {

    requires org.slf4j;
    requires kafka.clients;


    opens com.example.simplekafkaproducer to javafx.fxml;
    exports com.example.simplekafkaproducer;
    exports com.example.chapter3.consumer;
    opens com.example.chapter3.consumer to javafx.fxml;
    exports com.example.chapter3.producer;
    opens com.example.chapter3.producer to javafx.fxml;
    exports com.example.chapter3.producer.kafkaProducerAsyncCallback;
    opens com.example.chapter3.producer.kafkaProducerAsyncCallback to javafx.fxml;
    exports com.example.chapter3.producer.kafkaProducerCustomPartitioner;
    opens com.example.chapter3.producer.kafkaProducerCustomPartitioner to javafx.fxml;
    exports com.example.chapter3.consumer.simpleConsumer;
    opens com.example.chapter3.consumer.simpleConsumer to javafx.fxml;
}