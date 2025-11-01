package com.edu.ensias.kafka;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Scanner;

public class WordProducer {
    public static void main(String[] args) {
        if(args.length == 0){
            System.out.println("Usage: WordProducer <topic>");
            return;
        }

        String topicName = args[0];
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter words to send to Kafka (type 'exit' to quit):");
        while (true) {
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase("exit")) break;

            String[] words = line.split("\\s+");
            for(String word : words){
                producer.send(new ProducerRecord<>(topicName, word, word));
            }
        }

        producer.close();
        scanner.close();
    }
}
