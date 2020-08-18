package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run () {
        final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        String topic = "not-mow";
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "chelsea-application-thread";

        // Latch (for dealing with multiple threads)
        CountDownLatch latch = new CountDownLatch(1);

        // Create the consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited.");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted: ", e);
        } finally {
            logger.info("Application is closing...");
        }
    }

    public static class ConsumerRunnable implements Runnable {
        final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        private CountDownLatch latch;
        final KafkaConsumer<String, String> consumer;
        final String bootstrapServers;
        final String groupId;

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // "earliest", "latest", or "none"
            consumer = new KafkaConsumer<String, String>(properties);

            // Subscribe the consumer to our topic(s). By using Collections.singleton(), you only subscribe to one topic. If
            // you want to subscribe to MORE, use Arrays. EX: consumer.subscribe(Arrays.asList("m", "o", "w"));
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // Tell our main code that we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // The wakeup method is a special method to interrupt consumer.poll(), making it throw a WakeUpException
            consumer.wakeup();
        }
    }
}
