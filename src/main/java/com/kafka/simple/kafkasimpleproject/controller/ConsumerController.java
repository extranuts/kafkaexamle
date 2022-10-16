package com.kafka.simple.kafkasimpleproject.controller;


import com.kafka.simple.kafkasimpleproject.consumer.ManualConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/consumer")
public class ConsumerController {

    @Value("${kafka.first.topic}")
    private String FIRST_TOPIC;

    private final ManualConsumerService manualConsumerService;

    public ConsumerController(final ManualConsumerService manualConsumerService) {
        this.manualConsumerService = manualConsumerService;
    }


    @GetMapping("manual")//localhost:8080/api/consumer/manual?partition=0&offset=0
    public ResponseEntity<?> getMessageManually(
            @RequestParam(value = "partition", required = false, defaultValue = "0") Integer partition,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset
    ) {
        return ResponseEntity.ok(manualConsumerService.receiveMessages(FIRST_TOPIC, partition, offset));
    }

}
