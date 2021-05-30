package com.gmartinez.kafkatest.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

    @GetMapping("/getMessage")
    @KafkaListener(topics = "gustavo-topico", groupId = "foo")
    public String listenGroupFoo(String message) {
        log.info("Mensaje recibido en grupo Foo: " + message);
        return message;
    }

}
