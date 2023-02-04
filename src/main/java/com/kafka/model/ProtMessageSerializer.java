package com.kafka.model;

import com.kafka.message.ExchangeProtoMessage.ProtMessage;
import org.apache.kafka.common.serialization.Serializer;

public class ProtMessageSerializer implements Serializer<ProtMessage>{
    @Override
    public byte[] serialize(String topic, ProtMessage data) {
        return data.toByteArray();
    }
}
