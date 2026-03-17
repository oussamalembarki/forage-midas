package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        // 1: Validate sender and recipient exist
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return; // invalid — discard
        }

        // 2: Validate sender has enough balance
        if (sender.getBalance() < transaction.getAmount()) {
            return; // invalid — discard
        }

        // 3: Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        userRepository.save(sender);
        userRepository.save(recipient);

        // 4: Record the transaction
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
        transactionRepository.save(record);

        System.out.println("Transaction saved: " + transaction);
    }
}