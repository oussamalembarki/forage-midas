package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"trader-updates"})
public class TaskTwoTests {

    @Autowired
    private KafkaTemplate<String, Transaction> kafkaTemplate;

    @Value("${general.kafka-topic}")
    private String kafkaTopic;

    @Test
    void testTransactionListener() throws InterruptedException {
        Transaction transaction = new Transaction(1L, 2L, 100.0f);
        kafkaTemplate.send(kafkaTopic, transaction);

        System.out.println("Transaction sent. Waiting for consumer...");
        Thread.sleep(5000);
        System.out.println("Done! Check logs above for 'Received transaction'.");
    }
}