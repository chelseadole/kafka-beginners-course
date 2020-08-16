package com.github.chelseadole.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {

            String topic = "not-mow";
            String value = "hello world " + Integer.toString(i) + "!";
            String key = "id_" + Integer.toString(i);

            logger.info("Key: " + key);
            // id 0 --> partition 1
            // id_1 --> partition 0
            // id_2 --> partition 2
            // id_3 --> partition 0
            // id_4 --> partition 2
            // id_5 --> partition 2
            // id_6 --> partition 0
            // id_7 --> partition 2
            // id_8 --> partition 1
            // id_9 --> partition 2

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes each time a record is successfully sent, or an exception is raised
                    if (e == null) {
                        // The record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        // There was an error
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // .get() makes this synchronous. Do not use in prod, as it tanks performance.
        }

        // flush data
        producer.flush();
        producer.close();
    }
}
