package com.kafka.producer;

import com.kafka.message.ExchangeProtoMessage.ProtMessage;
import com.kafka.model.ProtMessageSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

public class MyKafkaProducerWithProtobufModel {

    public static void main(String[] args) {

        System.out.println("going to publish messages");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); //c:/windows/System32/drivers/etc/hosts
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, ProtMessage> producer = new KafkaProducer<>(props, new IntegerSerializer(), new ProtMessageSerializer());
        for (int i = 31; i <= 40; i++){
            producer.send(new ProducerRecord<>("myFirstTopic", 0, i, ProtMessage.newBuilder().setId(i).setName(i + "proto value").build()));
        }

        producer.close();
    }
}
