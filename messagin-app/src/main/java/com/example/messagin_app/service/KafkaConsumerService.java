package com.example.messagin_app.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    @KafkaListener(topics = "test-topic",groupId = "test-group")
    public void listen(String message){
        System.out.println("Recived message " + message);
    }
}
