package clients;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PreviousDataConsumer {
    public static void main(String[] args) {
        System.out.println("*** Starting Previous Data Consumer ***");
        Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "previous-data-consumer-v0.1.0");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        settings.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings);
        try {
            consumer.subscribe(Arrays.asList("hello-world-topic"));
            
            File offsetFile = new File("part0.txt");
            int offset = 0;
            if (offsetFile.exists()) {
                Scanner s = new Scanner(offsetFile);
                offset = s.nextInt();
            }
            Random r = new Random();
            int stopAfter = 1000 + r.nextInt(5000);
            // Always start from the beginning
            consumer.poll(0);
            consumer.seekToBeginning(consumer.assignment());

            for (int i=0; i<stopAfter; ++i) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n", 
                                      record.offset(), record.key(), record.value());
                }
            }
            
        }
        finally{
            consumer.close();
        }
    }
}
