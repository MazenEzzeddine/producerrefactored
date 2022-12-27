import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.UUID;

public class OldWorkload {

    public static void  startWorkload() throws IOException, URISyntaxException, InterruptedException {

        final Logger log = LogManager.getLogger(KafkaProducerExample.class);

        Workload wrld = new Workload();

        Random rnd = new Random();
        // over all the workload
        for (int i = 0; i < wrld.getDatax().size(); i++) {
            log.info("sending a batch of authorizations of size:{}",
                    Math.ceil(wrld.getDatay().get(i)));
            //   loop over each sample
            for (long j = 0; j < Math.ceil(wrld.getDatay().get(i)); j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
               KafkaProducerExample.
                       producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));


            }



            log.info("sent {} events Per Second ", Math.ceil(wrld.getDatay().get(i)));

            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }

}
