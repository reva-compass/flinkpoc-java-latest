package com.flink.poc.seattle.utils;

import com.flink.poc.seattle.entity.Condominium;
import com.flink.poc.utils.AvroUtils;
import com.flink.poc.utils.Utils;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Properties;

public class CondosManager implements Serializable {

    private static ObjectMapper mapper = new ObjectMapper();
    private static String condosTopic = "data_listings_mirrored_seattle_nwmls_condominium";
    private static String condosConsumerGroup = "condos-group";

    public Table processCondos(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {
        Properties properties = Utils.setProps();
        properties.setProperty("group.id", condosConsumerGroup);
        FlinkKafkaConsumer<byte[]> condosKafkaConsumer = new FlinkKafkaConsumer<>(condosTopic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        condosKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        // TODO comment below line so you dont read from beginning
        condosKafkaConsumer.setStartFromEarliest();
        DataStream<Condominium> condosStream = bsEnv.addSource(condosKafkaConsumer).map((MapFunction<byte[], Condominium>) data -> {
            Condominium condominium = new Condominium();
            GenericRecord message = AvroUtils.deSerializeMessageee(data);
            ByteBuffer bb = (ByteBuffer) message.get("payload");
            if (bb.hasArray()) {
                String converted = new String(bb.array(), "UTF-8");
                condominium.setDataL(converted);
                JsonNode jn = mapper.readTree(converted);
                condominium.setPkL(jn.get("LN").asText());
                // Agents
                if (jn.has("LAG"))
                    condominium.setListingAgent(jn.get("LAG").asText());
                if (jn.has("CLA"))
                    condominium.setCLA(jn.get("CLA").asText());
                if (jn.has("SAG"))
                    condominium.setSAG(jn.get("SAG").asText());
                if (jn.has("SCA"))
                    condominium.setSCA(jn.get("SCA").asText());
                // Offices
                if (jn.has("LO"))
                    condominium.setLO(jn.get("LO").asText());
                if (jn.has("COLO"))
                    condominium.setCOLO(jn.get("COLO").asText());
                if (jn.has("SO"))
                    condominium.setSO(jn.get("SO").asText());
                if (jn.has("SCO"))
                    condominium.setSCO(jn.get("SCO").asText());
            }
            return condominium;
        });

        String tableName = "Condos";
        bsTableEnv.registerDataStream(tableName, condosStream, "pkL," +
                "listingAgent," +
                "CLA," +
                "SAG," +
                "SCA," +
                "LO," +
                "COLO," +
                "SO," +
                "SCO," +
                "dataL," +
                "proctime.proctime");

        return bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY pkL ORDER BY proctime DESC) " +
                "AS row_num FROM " + tableName + ")" +
                "WHERE row_num = 1");
    }
}
