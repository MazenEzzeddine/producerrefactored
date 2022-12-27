import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class SimplerWorkload {

    public static void startWorkload() throws IOException, URISyntaxException, InterruptedException {
        final Logger log = LogManager.getLogger(KafkaProducerExample.class);
        Instant time = Instant.now();
        Random rnd = new Random();

        while(Duration.between(time, Instant.now()).getSeconds() < 120) {
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
        Integer tosend=90;
        while(Duration.between(time, Instant.now()).getSeconds() < 60) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
            tosend++;
        }


        time = Instant.now();
        while(Duration.between(time, Instant.now()).getSeconds() < 120) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }



    public static void startWorkload2() throws IOException, URISyntaxException, InterruptedException {
        final Logger log = LogManager.getLogger(KafkaProducerExample.class);
        Instant time = Instant.now();
        Random rnd = new Random();

        while(Duration.between(time, Instant.now()).getSeconds() < 120) {
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
        Integer tosend=90;
        while(Duration.between(time, Instant.now()).getSeconds() < 60) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
            tosend++;
        }


        time = Instant.now();
        while(Duration.between(time, Instant.now()).getSeconds() < 120) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }


        time = Instant.now();
         tosend=150;
        while(Duration.between(time, Instant.now()).getSeconds() < 60) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
            tosend++;
        }


        time = Instant.now();
        while(Duration.between(time, Instant.now()).getSeconds() < 120) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }


    public static void startWorkload3() throws IOException, URISyntaxException, InterruptedException {
        final Logger log = LogManager.getLogger(KafkaProducerExample.class);
        Instant time = Instant.now();
        Random rnd = new Random();

        Integer tosend=1;
        while(Duration.between(time, Instant.now()).getSeconds() < 90) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", 90);
            Thread.sleep(KafkaProducerExample.config.getDelay());
            tosend++;
        }


        time = Instant.now();


        while(Duration.between(time, Instant.now()).getSeconds() < 60) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }

        tosend=90;
        while(Duration.between(time, Instant.now()).getSeconds() < 60) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
            tosend++;
        }

        tosend= 150;
        time = Instant.now();
        while(Duration.between(time, Instant.now()).getSeconds() < 120) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }


    public static void startWorkload4() throws IOException, URISyntaxException, InterruptedException {
        final Logger log = LogManager.getLogger(KafkaProducerExample.class);
        Instant time = Instant.now();
        Random rnd = new Random();

        Integer tosend=1;
        while(Duration.between(time, Instant.now()).getSeconds() < 90) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", 90);
            Thread.sleep(KafkaProducerExample.config.getDelay());
            tosend++;
        }


        time = Instant.now();


        while(Duration.between(time, Instant.now()).getSeconds() < 60) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }

        tosend=90;
        while(Duration.between(time, Instant.now()).getSeconds() < 60) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
            tosend++;
        }

        tosend= 150;
        time = Instant.now();
        while(Duration.between(time, Instant.now()).getSeconds() < 120) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }



        while(Duration.between(time, Instant.now()).getSeconds() < 150) {
            //   loop over each sample
            for (long j = 0; j < tosend; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));

            }
            tosend--;
            log.info("sent {} events Per Second ", tosend);
            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }







}
