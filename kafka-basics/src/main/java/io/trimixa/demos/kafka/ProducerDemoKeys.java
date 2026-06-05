package io.trimixa.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    static void main() {
        log.info("Producer Demo");
        KafkaProducer<String, String> producer = getStringStringKafkaProducer();
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 30; i++) {

                String topic = "demo_java";
                String key = "id" + i;
                String value = "hello world" + i;

                //create the Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //send data
                producer.send(producerRecord, (metadata, exception) -> {
                    //execute everytime the record is sent or exception occurred
                    if (exception == null) {
                        //the record was successfully sent
                        log.info("Key:{} | Partition: {}", key, metadata.partition());
                    } else {
                        log.error("Error sending record", exception);
                    }
                });

            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //flush the producer: tell the producer to send all data and block until done --synchronous
        producer.flush();

        //close the producer
        producer.close();
    }

    private static KafkaProducer<String, String> getStringStringKafkaProducer() {
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //create the producer
        return new KafkaProducer<>(properties);
    }
}
