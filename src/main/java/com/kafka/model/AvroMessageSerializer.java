package com.kafka.model;

import com.kafka.message.AvroMessage;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroMessageSerializer implements Serializer<AvroMessage> {
    @Override
    public byte[] serialize(String topic, AvroMessage data) {

        byte[] arr = new byte[100000000];
        try {
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                GenericDatumWriter<AvroMessage> writer = new GenericDatumWriter<>(data.getSchema());
                writer.write(data, binaryEncoder);
                binaryEncoder.flush();
                arr = outputStream.toByteArray();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return arr;
    }
}
