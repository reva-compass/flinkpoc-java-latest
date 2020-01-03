package com.flink.poc.crmls;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;

public class JoinedSerializer implements KafkaSerializationSchema<Tuple2<Boolean, Row>> {

    private String topic;

    public JoinedSerializer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> serialize(Tuple2<Boolean, Row> booleanRowTuple2, Long aLong) {
        return new org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>(topic, booleanRowTuple2.toString().getBytes(StandardCharsets.UTF_8));
    }
}
