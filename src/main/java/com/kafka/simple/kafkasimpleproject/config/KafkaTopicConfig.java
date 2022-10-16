package com.kafka.simple.kafkasimpleproject.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import javax.annotation.PostConstruct;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.first.topic}")
    private String FIRST_TOPIC;

    @Value("${kafka.second.topic}")
    private String SECOND_TOPIC;
    @Value("${kafka.transactional-topic}")
    private String TRANSACTIONAL_TOPIC;

    private final KafkaAdmin kafkaAdmin;

    public KafkaTopicConfig(final KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    private NewTopic firstTopic(){
        return TopicBuilder.name(FIRST_TOPIC)
                .partitions(1)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "10000000")
                .build();
    }

    private NewTopic secondTopic(){
        return TopicBuilder.name(SECOND_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
    private NewTopic transactionalTopic(){
        return TopicBuilder.name(TRANSACTIONAL_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @PostConstruct
    public void init(){
        kafkaAdmin.createOrModifyTopics(firstTopic());
        kafkaAdmin.createOrModifyTopics(secondTopic());
        kafkaAdmin.createOrModifyTopics(transactionalTopic());
    }

}
