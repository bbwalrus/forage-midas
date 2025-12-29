package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {
    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public KafkaConsumer(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = new RestTemplate();
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        // check if sender, recipient is valid, and balance > amount
        UserRecord sender = databaseConduit.findById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            String url = "http://localhost:8080/incentive";
            Incentive incentive = restTemplate.postForObject(url, transaction, Incentive.class);
            float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0f;

            // update balances
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            // save updated user records
            databaseConduit.save(sender);
            databaseConduit.save(recipient);

            // save transaction record
            databaseConduit.saveTransaction(
                new com.jpmc.midascore.entity.TransactionRecord(
                    sender,
                    recipient,
                    transaction.getAmount()
                )
            );
            System.out.println(sender.getName() + " sent " + transaction.getAmount() + " to " + recipient.getName());
            System.out.println("New balance - " + sender.getName() + ": " + sender.getBalance() + ", " + recipient.getName() + ": " + recipient.getBalance());
        }
    }
}
