package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OddEvenNumProducerWithDefaultPartitioner {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        Producer<Integer, String> producer = new KafkaProducer<>(props, new IntegerSerializer(), new StringSerializer());
        Map<Integer, List<Integer>> partitionmap = new HashMap<>();

        for (int i = 0; i <= 10; i++){
            Future<RecordMetadata> response = producer.send(new ProducerRecord<>("oddEvenTopic", i, Integer.toString(i)));

            int partition = response.get().partition();
            List<Integer> list = partitionmap.getOrDefault(partition, new ArrayList<>());
            list.add(i);
            partitionmap.put(partition, list);
        }

        producer.close();

        for(int partition : partitionmap.keySet()){
            System.out.println("Parition id {"+partition+"} contains following keys : {"+partitionmap.get(partition)+"}");
        }
    }
}
