package com.example.kafka_producer_jwt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;



@Service
public final class ProducerService {
    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);
    private final KafkaTemplate<String,String> kafkaTemplate;
    private final String TOPIC = "myJava";

    public ProducerService(KafkaTemplate<String,String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String transcation){
        logger.info("The message produced is "+transcation);
        ListenableFuture<SendResult<String,String>> future = this.kafkaTemplate.send(TOPIC,transcation);
        future.addCallback(new ListenableFutureCallback<SendResult<String,String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.info(String.format("unable to send message =[{}] because {}",transcation.toString(),throwable.getMessage()));

            }

            @Override
            public void onSuccess(SendResult<String,String> stringStringSendResult) {
                logger.info(String.format("the message =[{}] has been successfully sent {}",transcation.toString(), stringStringSendResult.getRecordMetadata().offset()));
            }
        });
    }
}
