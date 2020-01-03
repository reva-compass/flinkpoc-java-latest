package com.flink.poc.crmls;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;


public class CrmlsJoiner {

    private static StreamExecutionEnvironment bsEnv;

    static {
        bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 1000 ms
        bsEnv.enableCheckpointing(1000);

        // make sure 500 ms of progress happen between checkpoints
        // bsEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within one minute, or are discarded
        // bsEnv.getCheckpointConfig().setCheckpointTimeout(60000);

        // allow job recovery fallback to checkpoint when there is a more recent savepoint
        // bsEnv.getCheckpointConfig().setPreferCheckpointForRecovery(true);
    }

    private static EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    private static StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
    private static ObjectMapper mapper = new ObjectMapper();
    private static FlinkKafkaProducer<Tuple2<Boolean, Row>> kafkaProducer =
            new FlinkKafkaProducer("test-topic", new JoinedSerializer("test-topic"), setProps(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

    private static Properties setProps() {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "b-2.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-1.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-3.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092");
        return properties;
    }

    private static Table processListings() {
        Properties properties = setProps();
        properties.setProperty("group.id", "listings-group");
        FlinkKafkaConsumer<ObjectNode> kafkaConsumer =
                new FlinkKafkaConsumer<>("la_crmls_rets-listings-neo", new JSONKeyValueDeserializationSchema(true), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<Listing> listingStream = bsEnv.addSource(kafkaConsumer).map((MapFunction<ObjectNode, Listing>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Listing listing = new Listing();
            if (jsonNode.has("uc_pk"))
                listing.setUcPK(jsonNode.get("uc_pk").textValue());
            if (jsonNode.has("uc_update_ts"))
                listing.setUcUpdateTS(jsonNode.get("uc_update_ts").textValue());
            if (jsonNode.has("uc_version"))
                listing.setUcVersion(jsonNode.get("uc_version").textValue());
            if (jsonNode.has("uc_row_type"))
                listing.setUcRowType(jsonNode.get("uc_row_type").textValue());
            if (jsonNode.has("uc_type"))
                listing.setUcType(jsonNode.get("uc_type").textValue());
            String dataStr = jsonNode.get("data").textValue();
            JsonNode dataNode = mapper.readTree(dataStr);
            listing.setData(dataStr);
            if (dataNode.has("ListingKeyNumeric"))
                listing.setListingKey(dataNode.get("ListingKeyNumeric").textValue());
            if (dataNode.has("ListAgentKeyNumeric"))
                listing.setListAgentKey(dataNode.get("ListAgentKeyNumeric").textValue());
            if (dataNode.has("BuyerAgentKeyNumeric"))
                listing.setBuyerAgentKey(dataNode.get("BuyerAgentKeyNumeric").textValue());
            if (dataNode.has("CoListAgentKeyNumeric"))
                listing.setCoListAgentKey(dataNode.get("CoListAgentKeyNumeric").textValue());
            if (dataNode.has("CoBuyerAgentKeyNumeric"))
                listing.setCoBuyerAgentKey(dataNode.get("CoBuyerAgentKeyNumeric").textValue());
            if (dataNode.has("ListOfficeKeyNumeric"))
                listing.setListOfficeKey(dataNode.get("ListOfficeKeyNumeric").textValue());
            if (dataNode.has("BuyerOfficeKeyNumeric"))
                listing.setBuyerOfficeKey(dataNode.get("BuyerOfficeKeyNumeric").textValue());
            if (dataNode.has("CoListOfficeKeyNumeric"))
                listing.setCoListOfficeKey(dataNode.get("CoListOfficeKeyNumeric").textValue());
            if (dataNode.has("CoBuyerOfficeKeyNumeric"))
                listing.setCoBuyerOfficeKey(dataNode.get("CoBuyerOfficeKeyNumeric").textValue());
            //  System.out.println("### list obj " + listing);
            return listing;
        });

        bsTableEnv.registerDataStream("Listings", listingStream, "ucPK, " +
                "ucUpdateTS, " +
                "ucVersion, " +
                "ucRowType, " +
                "ucType, " +
                "listingKey, " +
                "listAgentKey, " +
                "buyerAgentKey, " +
                "coListAgentKey, " +
                "coBuyerAgentKey, " +
                "listOfficeKey, " +
                "buyerOfficeKey, " +
                "coListOfficeKey, " +
                "coBuyerOfficeKey, " +
                "data," +
                "proctime.proctime");

        Table latestListingsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY listingKey ORDER BY proctime DESC) " +
                "AS row_num FROM Listings)" +
                "WHERE row_num = 1");
        return latestListingsTbl;
    }

    private static Table processAgents() {
        Properties properties = setProps();
        properties.setProperty("group.id", "agents-group");
        FlinkKafkaConsumer<ObjectNode> agentKafkaConsumer = new FlinkKafkaConsumer<>("la_crmls_rets-agents-neo", new JSONKeyValueDeserializationSchema(true), properties);
        agentKafkaConsumer.setStartFromEarliest();
        DataStream<Agent> agentStream = bsEnv.addSource(agentKafkaConsumer).map((MapFunction<ObjectNode, Agent>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Agent agent = new Agent();
            if (jsonNode.has("uc_pk"))
                agent.setUcPK(jsonNode.get("uc_pk").textValue());
            if (jsonNode.has("uc_update_ts"))
                agent.setUcUpdateTS(jsonNode.get("uc_update_ts").textValue());
            if (jsonNode.has("uc_version"))
                agent.setUcVersion(jsonNode.get("uc_version").textValue());
            if (jsonNode.has("uc_row_type"))
                agent.setUcRowType(jsonNode.get("uc_row_type").textValue());
            if (jsonNode.has("uc_type"))
                agent.setUcType(jsonNode.get("uc_type").textValue());
            agent.setData(jsonNode.get("data").textValue());
            return agent;
        });

        bsTableEnv.registerDataStream("Agents", agentStream, "ucPK, " +
                "ucUpdateTS, " +
                "ucVersion, " +
                "ucRowType, " +
                "ucType, " +
                "data," +
                "proctime.proctime");

        Table latestAgentsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY ucPK ORDER BY proctime DESC) " +
                "AS row_num FROM Agents)" +
                "WHERE row_num = 1");

        return latestAgentsTbl;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("### inside main");

        /**
         * LISTINGS
         */
        Table latestListingsTbl = processListings();
        bsTableEnv.registerTable("latestListings", latestListingsTbl);
//        Table result3 = bsTableEnv.sqlQuery(
//                "SELECT * FROM latestListings");
//        bsTableEnv.toRetractStream(result3, Row.class).print();

        /**
         * AGENTS
         */
        Table latestAgentsTbl = processAgents();
        bsTableEnv.registerTable("latestAgents", latestAgentsTbl);
//        Table result4 = bsTableEnv.sqlQuery(
//                "SELECT * FROM latestAgents");
//        bsTableEnv.toRetractStream(result4, Row.class).print();

        /**
         * JOIN
         */
        Table joinedTbl = bsTableEnv.sqlQuery(
                "SELECT * FROM latestListings l " +
                        "LEFT JOIN latestAgents aa ON l.listAgentKey = aa.ucPK " +
                        "LEFT JOIN latestAgents ab ON l.buyerAgentKey = ab.ucPK " +
                        "LEFT JOIN latestAgents ac ON l.coListAgentKey = ac.ucPK " +
                        "LEFT JOIN latestAgents ad ON l.coBuyerAgentKey = ad.ucPK"
        );
        DataStream<Tuple2<Boolean, Row>> joinedStream = bsTableEnv.toRetractStream(joinedTbl, Row.class);
        joinedStream.addSink(kafkaProducer);
        bsEnv.execute("test-job");

    }


}
