package com.kafka.producer;

import com.kafka.message.AvroMessage;
import com.kafka.model.AvroMessageSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

public class MyKafkaProducerWithAvroSerializer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        Producer<Integer, AvroMessage> producer = new KafkaProducer<>(props, new IntegerSerializer(), new AvroMessageSerializer());
        for (int i = 1; i <= 100; i++){
            producer.send(new ProducerRecord<>("myFirstTopic", 0, i, AvroMessage.newBuilder().setId(i).setName(i + "avro value").build()));
        }

        producer.close();
    }
}
