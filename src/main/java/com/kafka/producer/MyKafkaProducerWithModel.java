package com.kafka.producer;

import com.kafka.model.KafkaMessage;
import com.kafka.model.ModelSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyKafkaProducerWithModel {

    public static void main(String[] args) {

        System.out.println("going to publish messages");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        Producer<Integer, KafkaMessage> producer = new KafkaProducer<Integer, KafkaMessage>(props, new IntegerSerializer(), new ModelSerializer());
        for (int i = 31; i <= 40; i++){
            producer.send(new ProducerRecord<>("myFirstTopic", 0, i, new KafkaMessage(i, "value" + i)));
        }

        producer.close();
    }
}
