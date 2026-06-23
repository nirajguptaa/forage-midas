
package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        }
)
public class TaskThreeTests {

    static final Logger logger =
            LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
private DatabaseConduit databaseConduit;

@Test
void task_three_verifier() throws InterruptedException {

    userPopulator.populate();

    String[] transactionLines =
            fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");

    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }

    Thread.sleep(3000);

    System.out.println("\n===== USERS =====");

    for(long id = 1; id <= 20; id++) {

        UserRecord user = databaseConduit.findUser(id);

        if(user != null) {
            System.out.println(
                    user.getName()
                    + " -> "
                    + user.getBalance()
            );
        }
    }
}
}