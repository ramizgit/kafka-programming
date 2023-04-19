package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class MyKafkaConsumerGroup {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        TopicPartition p1 = new TopicPartition("myFirstTopic", 0);
        TopicPartition p2 = new TopicPartition("myFirstTopic", 1);
        TopicPartition p3 = new TopicPartition("myFirstTopic", 3);

        //consumer1
        KafkaConsumer consumer1 = new KafkaConsumer(props);
        consumer1.assign(Arrays.asList(p1, p2));

        //consumer2
        KafkaConsumer consumer2 = new KafkaConsumer(props);
        consumer2.assign(Arrays.asList(p2, p3));

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
