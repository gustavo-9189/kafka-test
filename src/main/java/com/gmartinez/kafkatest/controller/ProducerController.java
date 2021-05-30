package com.gmartinez.kafkatest.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public ProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/message/{message}")
    public void sendMessage(@PathVariable String message) {
        kafkaTemplate.send("gustavo-topico", message);
    }

    @PostMapping("/message/{topic}/{message}")
    public void sendMessage(@PathVariable String topic, @PathVariable String message) {
        kafkaTemplate.send(topic, message);
    }

}
