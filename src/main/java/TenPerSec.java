import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class TenPerSec {

    public static void startWorkload() throws IOException, URISyntaxException, InterruptedException {

        final Logger log = LogManager.getLogger(TenPerSec.class);
        Instant start = Instant.now();
        Random rnd = new Random();


        while (Duration.between(start, Instant.now()).getSeconds() < 10  * 60) {


            log.info("sending a batch of authorizations of size: 10");
            //   loop over each sample
            for (long j = 0; j < 50; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));
                //log.info("sent 10 events Per Second ");

            }
            Thread.sleep(KafkaProducerExample.config.getDelay());

        }

    }
}