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

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(
        ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka Consumer");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "demo-java";

        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // none, do not read if there is no current offset
        // earliest, read from the smallest offsets (aka the oldest ones in time), which gives all the data.
        // latest, read from the largest offsets (aka the most recent ones in time), which gives only the latest data.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread

                try{
                    mainThread.join();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
            // subscribe consumer to our topics
            consumer.subscribe(Collections.singletonList(topic)); // One topic
//        consumer.subscribe(Arrays.asList(topic)); // multiple topics


            // poll for new data

            Duration timeoutIn = Duration.ofMillis(1000);
            while(true) {

                log.info("Polling");

                // poll handles our waiting so the there is no busy-waiting
                ConsumerRecords<String, String> records = consumer.poll(timeoutIn);

                // iterate over all our records
                for(ConsumerRecord<String, String> record: records) {
                    // do stuff with our consumed record
                    log.info("Key: " + record.key() + ", Value " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset " + record.offset());
                }


            }
        } catch (WakeupException e) {
            log.info("Wakeup exception!");
        } catch (Exception e) {
            log.error("Unexpected Exception");
        } finally {
            consumer.close(); // this will also commit the offsets if need be
            log.info("The consumer is now gracefully closed");
        }




    }
}
