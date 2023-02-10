package com.kafka.consumer;

import com.kafka.message.AvroMessage;
import com.kafka.model.AvroMessageDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyKafkaConsumerWithAvroSerializer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");

        KafkaConsumer<Integer, AvroMessage> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new AvroMessageDeserializer());
        consumer.subscribe(Arrays.asList("myFirstTopic"));

        while (true) {
            ConsumerRecords<Integer, AvroMessage> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<Integer, AvroMessage> record : records) {
                System.out.println("Received message: (" + record.key() + ", " + record.value().toString() + ") at offset " + record.offset());
            }
        }
    }
}
