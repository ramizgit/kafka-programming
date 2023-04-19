package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaConsumerGroupExample {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "myConsumerGroup");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //consumer 1
        KafkaConsumer consumer1 = new KafkaConsumer(props);
        consumer1.subscribe(Arrays.asList("myFirstTopic"));

        //consumer 2
        KafkaConsumer consumer2 = new KafkaConsumer(props);
        consumer2.subscribe(Arrays.asList("myFirstTopic"));

        System.out.println("consumer1 grp id : "+consumer1.groupMetadata().groupId());
        System.out.println("consumer2 grp id : "+consumer2.groupMetadata().groupId());

        while (true) {
            ConsumerRecords records = consumer1.poll(Duration.ofMillis(100));
            Iterator iterator = records.iterator();

            while (iterator.hasNext()){
                ConsumerRecord record = (ConsumerRecord) iterator.next();
                System.out.printf("consumer1 offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }

            ConsumerRecords records2 = consumer2.poll(Duration.ofMillis(100));
            Iterator iterator2 = records2.iterator();

            while (iterator2.hasNext()){
                ConsumerRecord record = (ConsumerRecord) iterator2.next();
                System.out.printf("consumer2 offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
