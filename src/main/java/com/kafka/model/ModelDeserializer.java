package com.kafka.model;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ModelDeserializer implements Deserializer<KafkaMessage>{
    @Override
    public KafkaMessage deserialize(String topic, byte[] data) {

        KafkaMessage model = new KafkaMessage();

        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            model = (KafkaMessage) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return model;
    }
}
