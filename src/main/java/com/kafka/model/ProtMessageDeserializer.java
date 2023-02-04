package com.kafka.model;

import com.google.protobuf.InvalidProtocolBufferException;
import com.kafka.message.ExchangeProtoMessage.ProtMessage;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtMessageDeserializer implements Deserializer<ProtMessage>{
    @Override
    public ProtMessage deserialize(String topic, byte[] data) {
        try {
            return ProtMessage.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new RuntimeException("excepiton while parsing");
        }
    }
}
