package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String[] args) {

        System.out.println("going to publish messages");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 31; i <= 40; i++){
            producer.send(new ProducerRecord<String, String>("myFirstTopic", 1, Integer.toString(i), Integer.toString(i)));
        }
            
        producer.close();
    }
}
