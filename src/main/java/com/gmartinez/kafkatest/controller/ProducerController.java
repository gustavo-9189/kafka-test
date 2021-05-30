package com.gmartinez.kafkatest.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private static final Logger log = LoggerFactory.getLogger(ProducerController.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public ProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/message/{message}")
    public void sendMessage(@PathVariable String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("gustavo-topico", message);
        futureMessage(future, message);
    }

    @PostMapping("/message/{topic}/{message}")
    public void sendMessage(@PathVariable String topic, @PathVariable String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        futureMessage(future, message);
    }

    private void futureMessage(ListenableFuture<SendResult<String, String>> future, String message) {
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Envio de mensaje: " + message + " con offset: " + result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("No se puede enviar el mensaje: " +  message + " tipo error: " + ex.getMessage());
            }
        });
    }

}
