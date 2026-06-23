package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private final DatabaseConduit databaseConduit;

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(Transaction transaction) {
         System.out.println(

            "Sender=" + transaction.getSenderId()

            + ", Recipient=" + transaction.getRecipientId()

            + ", Amount=" + transaction.getAmount()

    );


        UserRecord sender =
                databaseConduit.findUser(transaction.getSenderId());

        UserRecord recipient =
                databaseConduit.findUser(transaction.getRecipientId());

        if (sender == null || recipient == null)
            return;

        if (sender.getBalance() < transaction.getAmount())
            return;

        sender.setBalance(
                sender.getBalance() - transaction.getAmount()
        );

        recipient.setBalance(
                recipient.getBalance() + transaction.getAmount()
        );

        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        databaseConduit.saveTransaction(
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount()
                )
        );
    }
}