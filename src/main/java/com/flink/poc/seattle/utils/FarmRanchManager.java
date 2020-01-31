package com.flink.poc.seattle.utils;

import com.flink.poc.seattle.entity.FarmRanch;
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

public class FarmRanchManager implements Serializable {

    private static ObjectMapper mapper = new ObjectMapper();
    private static String farmranchTopic = "data_listings_mirrored_seattle_nwmls_farmranch";
    private static String farmranchConsumerGroup = "farmranch-group";


    public Table processFarmRanch(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {
        Properties properties = Utils.setProps();
        properties.setProperty("group.id", farmranchConsumerGroup);
        FlinkKafkaConsumer<byte[]> farmRanchKafkaConsumer = new FlinkKafkaConsumer<>(farmranchTopic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        farmRanchKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        // TODO comment below line so you dont read from beginning
        farmRanchKafkaConsumer.setStartFromEarliest();
        DataStream<FarmRanch> listingsStream = bsEnv.addSource(farmRanchKafkaConsumer).map((MapFunction<byte[], FarmRanch>) data -> {
            FarmRanch farmRanch = new FarmRanch();
            GenericRecord message = AvroUtils.deSerializeMessageee(data);
            ByteBuffer bb = (ByteBuffer) message.get("payload");
            if (bb.hasArray()) {
                String converted = new String(bb.array(), "UTF-8");
                farmRanch.setDataL(converted);
                JsonNode jn = mapper.readTree(converted);
                farmRanch.setPkL(jn.get("LN").asText());
                // Agents
                if (jn.has("LAG"))
                    farmRanch.setListingAgent(jn.get("LAG").asText());
                if (jn.has("CLA"))
                    farmRanch.setCLA(jn.get("CLA").asText());
                if (jn.has("SAG"))
                    farmRanch.setSAG(jn.get("SAG").asText());
                if (jn.has("SCA"))
                    farmRanch.setSCA(jn.get("SCA").asText());
                // Offices
                if (jn.has("LO"))
                    farmRanch.setLO(jn.get("LO").asText());
                if (jn.has("COLO"))
                    farmRanch.setCOLO(jn.get("COLO").asText());
                if (jn.has("SO"))
                    farmRanch.setSO(jn.get("SO").asText());
                if (jn.has("SCO"))
                    farmRanch.setSCO(jn.get("SCO").asText());
            }
            return farmRanch;
        });

        String tableName = "FarmRanch";
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
