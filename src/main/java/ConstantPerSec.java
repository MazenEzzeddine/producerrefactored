import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class ConstantPerSec {
    public static void startWorkload() throws IOException, URISyntaxException, InterruptedException {
        final Logger log = LogManager.getLogger(KafkaProducerExample.class);
        Random rnd = new Random();
        // over all the workload
        for (int i = 0; i < 300; i++) {
            /*log.info("sending a batch of authorizations of size:{}",
                    //Math.ceil(wrld.getDatay().get(i)));*/
            //   loop over each sample
            for (long j = 0; j < 90; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", 90);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }

    public static void startWorkload2() throws IOException, URISyntaxException, InterruptedException {
        final Logger log = LogManager.getLogger(KafkaProducerExample.class);
        Instant time = Instant.now();
        Random rnd = new Random();

        while(Duration.between(time, Instant.now()).getSeconds() < 180) {
            //   loop over each sample
            for (long j = 0; j < 90; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", 90);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
        time = Instant.now();
        while(Duration.between(time, Instant.now()).getSeconds() < 120) {
            //   loop over each sample
            for (long j = 0; j < 120; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", 120);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }


        time = Instant.now();
        while(Duration.between(time, Instant.now()).getSeconds() < 180) {
            //   loop over each sample
            for (long j = 0; j < 50; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", 50);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }
}




