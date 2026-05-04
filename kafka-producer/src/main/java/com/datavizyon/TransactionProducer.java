package com.datavizyon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionProducer {

    private static final String TOPIC = System.getenv().getOrDefault("KAFKA_TOPIC", "transactions");
    private static final String BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092");
    private static final long INTERVAL_MS = Long.parseLong(System.getenv().getOrDefault("INTERVAL_MS", "500"));

    private static final List<String> CATEGORIES = Arrays.asList(
        "retail", "food", "travel", "tech", "health", "finance", "entertainment"
    );
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Random RANDOM = ThreadLocalRandom.current();

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

            System.out.println("Starting producer to " + BOOTSTRAP + " topic=" + TOPIC);
            long seq = 0;
            while (!Thread.interrupted()) {
                String json = generateTransaction(seq++);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, json);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Send error: " + exception.getMessage());
                    }
                });
                if (seq % 100 == 0) {
                    System.out.println("Sent batch " + seq);
                }
                Thread.sleep(INTERVAL_MS);
            }
        }
    }

    private static String generateTransaction(long seq) {
        ObjectNode tx = MAPPER.createObjectNode();
        tx.put("id", UUID.randomUUID().toString());
        tx.put("seq", seq);
        tx.put("timestamp", Instant.now().toEpochMilli());
        tx.put("amount", Math.round(RANDOM.nextDouble() * 10000 * 100.0) / 100.0);
        tx.put("category", CATEGORIES.get(RANDOM.nextInt(CATEGORIES.size())));
        tx.put("user_id", "user_" + RANDOM.nextInt(1000));
        tx.put("currency", "USD");
        tx.put("status", RANDOM.nextDouble() < 0.95 ? "completed" : "failed");
        return tx.toString();
    }
}
