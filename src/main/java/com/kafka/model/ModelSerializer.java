package com.kafka.model;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ModelSerializer implements Serializer<KafkaMessage>{
    @Override
    public byte[] serialize(String topic, KafkaMessage data) {

        byte[] arr = new byte[100000000];
        try {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(data);
                oos.flush();
                arr = bos.toByteArray();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return arr;
    }
}
