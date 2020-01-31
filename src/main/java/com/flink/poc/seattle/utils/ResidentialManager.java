package com.flink.poc.seattle.utils;

import com.flink.poc.seattle.entity.Residential;
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

public class ResidentialManager implements Serializable {

    private static ObjectMapper mapper = new ObjectMapper();
    private static String residentialTopic = "data_listings_mirrored_seattle_nwmls_residential";
    private static String residentialConsumerGroup = "residential-group";

    public Table processResidential(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {
        Properties properties = Utils.setProps();
        properties.setProperty("group.id", residentialConsumerGroup);
        FlinkKafkaConsumer<byte[]> residentialKafkaConsumer = new FlinkKafkaConsumer<>(residentialTopic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        residentialKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        // TODO comment below line so you dont read from beginning
        residentialKafkaConsumer.setStartFromEarliest();
        DataStream<Residential> listingsStream = bsEnv.addSource(residentialKafkaConsumer).map((MapFunction<byte[], Residential>) data -> {
            Residential residential = new Residential();
            GenericRecord message = AvroUtils.deSerializeMessageee(data);
            ByteBuffer bb = (ByteBuffer) message.get("payload");
            if (bb.hasArray()) {
                String converted = new String(bb.array(), "UTF-8");
                residential.setDataL(converted);
                JsonNode jn = mapper.readTree(converted);
                residential.setPkL(jn.get("LN").asText());
                // Agents
                if (jn.has("LAG"))
                    residential.setListingAgent(jn.get("LAG").asText());
                if (jn.has("CLA"))
                    residential.setCLA(jn.get("CLA").asText());
                if (jn.has("SAG"))
                    residential.setSAG(jn.get("SAG").asText());
                if (jn.has("SCA"))
                    residential.setSCA(jn.get("SCA").asText());
                // Offices
                if (jn.has("LO"))
                    residential.setLO(jn.get("LO").asText());
                if (jn.has("COLO"))
                    residential.setCOLO(jn.get("COLO").asText());
                if (jn.has("SO"))
                    residential.setSO(jn.get("SO").asText());
                if (jn.has("SCO"))
                    residential.setSCO(jn.get("SCO").asText());
            }
            return residential;
        });
        String tableName = "Residential";
        bsTableEnv.registerDataStream(tableName, listingsStream, "pkL," +
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
