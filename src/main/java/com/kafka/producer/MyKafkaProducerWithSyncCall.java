package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyKafkaProducerWithSyncCall {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        Producer<String, String> producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());
        for (int i = 31; i <= 40; i++){
            Future<RecordMetadata> response = producer.send(new ProducerRecord<>("myFirstTopic", Integer.toString(i), Integer.toString(i)));
            System.out.println("Successfully written message to topic : {"+response.get().topic()+"}");
            System.out.println("Successfully written message to partition : {"+response.get().partition()+"}");
            System.out.println("Successfully written message with offset : {"+response.get().offset()+"}");
            System.out.println("Successfully written message with serializedKeySize : {"+response.get().serializedKeySize()+"}");
            System.out.println("Successfully written message with serializedValueSize : {"+response.get().serializedValueSize()+"}");
        }
        producer.close();
    }
}
