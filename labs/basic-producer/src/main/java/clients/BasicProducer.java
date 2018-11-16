package clients;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class BasicProducer {
    public static void main(String[] args) {
        System.out.println("*** Starting Basic Producer ***");

        // here will be the Kafka Producer code
        Properties props = new Properties();
        props.put("client.id", "basic-producer");
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping Basic Producer ###");
            producer.close();
            }));
        final String topic = "helloworld-topic";
        for (int i=0;;++i) {
            final String key_i = "key-" + i;
            final String val_i = "val-" + i;
            System.out.println(key_i + ": " + val_i);
            producer.send(new ProducerRecord<String, String>(topic, key_i, val_i));
        }
    }
}