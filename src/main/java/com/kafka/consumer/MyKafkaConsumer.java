package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class MyKafkaConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer consumer = new KafkaConsumer(props);
        //consumer.subscribe(Arrays.asList("myFirstTopic"));

        TopicPartition p1 = new TopicPartition("myFirstTopic", 0);
        TopicPartition p2 = new TopicPartition("myFirstTopic", 1);
        consumer.assign(Arrays.asList(p1, p2));

        while (true) {
            ConsumerRecords records = consumer.poll(Duration.ofMillis(100));
            Iterator iterator = records.iterator();

            while (iterator.hasNext()){
                ConsumerRecord record = (ConsumerRecord) iterator.next();
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }

            /*for (ConsumerRecord record: records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            */
        }
    }
}
