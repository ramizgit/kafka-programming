package com.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyKafkaProducerWithCallBack {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("linger.ms", 1);

        Producer<String, String> producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());
        for (int i = 31; i <= 40; i++){
            producer.send(new ProducerRecord<>("myFirstTopic", Integer.toString(i), Integer.toString(i)), new ProducerCallback());
        }
        producer.close();
    }
}

class ProducerCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            exception.printStackTrace();
        }

        System.out.println("Successfully returned from a callback!!!");
        System.out.println("Successfully written message to topic : {"+metadata.topic()+"}");
        System.out.println("Successfully written message to partition : {"+metadata.partition()+"}");
        System.out.println("Successfully written message with offset : {"+metadata.offset()+"}");
        System.out.println("Successfully written message with serializedKeySize : {"+metadata.serializedKeySize()+"}");
        System.out.println("Successfully written message with serializedValueSize : {"+metadata.serializedValueSize()+"}");
    }
}
