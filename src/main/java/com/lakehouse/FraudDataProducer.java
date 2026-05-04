package com.lakehouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class FraudDataProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap:9092");
        String topic = "fraud-transactions";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                ObjectNode transaction = mapper.createObjectNode();
                transaction.put("transaction_id", UUID.randomUUID().toString());
                transaction.put("user_id", "user_" + random.nextInt(1000));
                transaction.put("amount", Math.round(random.nextDouble() * 10000.0) / 100.0);
                transaction.put("timestamp", System.currentTimeMillis());
                
                // Anomali/Fraud tespiti icin rastgele %5 ihtimalle fraud islemi uret
                boolean isFraud = random.nextDouble() < 0.05;
                transaction.put("is_fraud", isFraud);

                String jsonValue = transaction.toString();
                producer.send(new ProducerRecord<>(topic, jsonValue));
                
                System.out.println("Sent: " + jsonValue);
                Thread.sleep(500); // Saniyede 2 islem
            }
        }
    }
}
