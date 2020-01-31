package com.flink.poc.seattle.utils;

import com.flink.poc.seattle.entity.VacantLand;
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

public class VacantLandManager implements Serializable {

    private static ObjectMapper mapper = new ObjectMapper();
    private static String vacantLandTopic = "data_listings_mirrored_seattle_nwmls_vacantland";
    private static String vacantLandConsumerGroup = "vacant-land-group";

    public Table processVacantLand(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {
        Properties properties = Utils.setProps();
        properties.setProperty("group.id", vacantLandConsumerGroup);
        FlinkKafkaConsumer<byte[]> vacantLandKafkaConsumer = new FlinkKafkaConsumer<>(vacantLandTopic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        vacantLandKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        // TODO comment below line so you dont read from beginning
        vacantLandKafkaConsumer.setStartFromEarliest();
        DataStream<VacantLand> vacantLandStream = bsEnv.addSource(vacantLandKafkaConsumer).map((MapFunction<byte[], VacantLand>) data -> {
            VacantLand vacantLand = new VacantLand();
            GenericRecord message = AvroUtils.deSerializeMessageee(data);
            ByteBuffer bb = (ByteBuffer) message.get("payload");
            if (bb.hasArray()) {
                String converted = new String(bb.array(), "UTF-8");
                vacantLand.setDataL(converted);
                JsonNode jn = mapper.readTree(converted);
                vacantLand.setPkL(jn.get("LN").asText());
                // Agents
                if (jn.has("LAG"))
                    vacantLand.setListingAgent(jn.get("LAG").asText());
                if (jn.has("CLA"))
                    vacantLand.setCLA(jn.get("CLA").asText());
                if (jn.has("SAG"))
                    vacantLand.setSAG(jn.get("SAG").asText());
                if (jn.has("SCA"))
                    vacantLand.setSCA(jn.get("SCA").asText());
                // Offices
                if (jn.has("LO"))
                    vacantLand.setLO(jn.get("LO").asText());
                if (jn.has("COLO"))
                    vacantLand.setCOLO(jn.get("COLO").asText());
                if (jn.has("SO"))
                    vacantLand.setSO(jn.get("SO").asText());
                if (jn.has("SCO"))
                    vacantLand.setSCO(jn.get("SCO").asText());
            }
            return vacantLand;
        });

        String tableName = "VancantLand";
        bsTableEnv.registerDataStream(tableName, vacantLandStream, "pkL," +
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
