package com.kafka.simple.kafkasimpleproject.controller;

import com.kafka.simple.kafkasimpleproject.model.MessageEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Slf4j
@RestController
@RequestMapping("api/producer")
public class ProducerController {

    @Value("${kafka.first.topic}")
    private String FIRST_TOPIC;

    @Value("${kafka.second.topic}")
    private String SECOND_TOPIC;

    @Value("${kafka.transactional-topic}")
    private String TRANSACTIONAL_TOPIC;


    @Autowired
    private KafkaTemplate<String, Object> kafkaProducerTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTransactionalProducerTemplate;

    @PostMapping("send")
    public ResponseEntity<?> sendMessage() {
        MessageEntity messageEntity = new MessageEntity("default", LocalDateTime.now());

        ListenableFuture<SendResult<String, Object>> future = kafkaProducerTemplate.send(FIRST_TOPIC, messageEntity);

//        SendResult<String, Object> result = future.get();
//        log.info("Send message with offset: {}, partition: {}", result.getRecordMetadata().offset(), result.getRecordMetadata().partition());

        //Async RESULT !!
        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(final Throwable ex) {
                log.error("Unable to send message {}", ex.getMessage());
            }

            @Override
            public void onSuccess(final SendResult<String, Object> result) {
                log.info("Send message with offset: {}, partition: {}", result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
            }
        });

        return ResponseEntity.ok(messageEntity);
    }

    @PostMapping("send-with-key/{key}")
    public ResponseEntity<?> sendMessageWithKey(@PathVariable String key) throws ExecutionException, InterruptedException {
        MessageEntity messageEntity = new MessageEntity("record-key", LocalDateTime.now());

        ListenableFuture<SendResult<String, Object>> future = kafkaProducerTemplate.send(SECOND_TOPIC, key, messageEntity);

        SendResult<String, Object> result = future.get();
        log.info("Sent message with offset: {}, partition: {}", result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
        return ResponseEntity.ok(messageEntity);
    }

    @PostMapping("transactional/{key}")
    public ResponseEntity<?> sendTransaction(@PathVariable String key) {
        List<MessageEntity> entityList = new ArrayList<>();

        kafkaTransactionalProducerTemplate.executeInTransaction(kafkaOperations -> {
            String[] keyList = key.split(",");
            for (String str : keyList) {
                if ("success".equals(str)) {
                    MessageEntity messageEntity = new MessageEntity(str, LocalDateTime.now());
                    kafkaOperations.send(TRANSACTIONAL_TOPIC,str,messageEntity);
                    entityList.add(messageEntity);
                }
                else throw new RuntimeException();
            }
            return Optional.of("warning !! null statement");
        });
        return ResponseEntity.ok(entityList);
    }
}
