package io.trimixa.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Demo");
        KafkaProducer<String, String> producer = getStringStringKafkaProducer();

        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++) {
                //create the Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic", "key", "value" + i);
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        //execute everytime the record is sent or exception occurred
                        if (exception == null) {
                            //the record was successfully sent
                            log.info("Received new metadata\nTopic:{}\nPartition:{}\nOffset:{}\nTimestamp:{}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                        } else {
                            log.error("Error sending record", exception);
                        }
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
        properties.setProperty("batch.size", "400");
        //create the producer
        return new KafkaProducer<>(properties);
    }
}
