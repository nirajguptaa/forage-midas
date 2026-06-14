package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(Transaction transaction) {

        System.out.println(transaction);

    }
}