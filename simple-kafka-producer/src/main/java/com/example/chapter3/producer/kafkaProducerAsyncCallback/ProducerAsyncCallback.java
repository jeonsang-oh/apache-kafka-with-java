package com.example.chapter3.producer.kafkaProducerAsyncCallback;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;

public class ProducerAsyncCallback implements Callback {
    private final static Logger logger = LoggerFactory.getLogger(ProducerAsyncCallback.class);

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                logger.error(e.getMessage(), e);
            } else {
                logger.info(recordMetadata.toString());
            }
        }
    }

