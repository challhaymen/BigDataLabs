package com.edu.ensias.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {

    public static void main(String[] args) throws Exception {

        // Vérifier que le nom du topic est fourni comme argument
        if (args.length == 0) {
            System.out.println("Veuillez entrer le nom du topic en argument !");
            System.out.println("Exemple : java -jar target/kafka-consumer-app-jar-with-dependencies.jar my-topic");
            return;
        }

        // Récupérer le nom du topic passé en paramètre
        String topicName = args[0];

        // Créer les propriétés du producteur Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Adresse du broker Kafka
        props.put("acks", "all");                         // Acquittement complet pour fiabilité
        props.put("retries", 0);                          // Pas de tentative de réessai
        props.put("batch.size", 16384);                   // Taille du lot en octets
        props.put("buffer.memory", 33554432);             // Mémoire tampon disponible
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Créer le producteur Kafka
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Envoyer 10 messages dans le topic
        for (int i = 0; i < 10; i++) {
            String key = Integer.toString(i);
            String value = "Message numéro " + i;
            producer.send(new ProducerRecord<>(topicName, key, value));
        }

        System.out.println("✅ Messages envoyés avec succès dans le topic : " + topicName);

        // Fermer le producteur
        producer.close();
    }
}
