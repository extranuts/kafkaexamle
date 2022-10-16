package com.kafka.simple.kafkasimpleproject.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageEntity {

    private String type;
    private LocalDateTime time;

}
