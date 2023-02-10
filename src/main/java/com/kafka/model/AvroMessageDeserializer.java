package com.kafka.model;

import com.kafka.message.AvroMessage;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroMessageDeserializer implements Deserializer<AvroMessage> {
    @Override
    public AvroMessage deserialize(String topic, byte[] data) {

        try {
            if (data != null) {
                DatumReader<AvroMessage> reader = new SpecificDatumReader<>(AvroMessage.getClassSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                return reader.read(null, decoder);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
