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
import java.util.Random;


public class CrmlsJoiner {

    private static StreamExecutionEnvironment bsEnv;

    static {
        bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 1000 ms
        bsEnv.enableCheckpointing(2000);

        // make sure 500 ms of progress happen between checkpoints
        bsEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(1500);

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
    private static Random rnd = new Random();

    /* TOPICS */
    private static String listingsTopic = "la_crmls_rets-listings-p";
    private static String agentsTopic = "la_crmls_rets-agents-p";
    private static String officesTopic = "la_crmls_rets-offices-p";
    private static String openHousesTopic = "la_crmls_rets-openhouses-p";

    /* CONSUMER GROUPS */
    private static String listingsConsumerGroup = "";
    private static String agentsConsumerGroup = "";
    private static String officesConsumerGroup = "";
    private static String openHousesConsumerGroup = "";

    private static String COMPASS_NULL = "-COMPASS-NULL";

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
                new FlinkKafkaConsumer<>(listingsTopic, new JSONKeyValueDeserializationSchema(true), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<Listing> listingStream = bsEnv.addSource(kafkaConsumer).map((MapFunction<ObjectNode, Listing>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Listing listing = new Listing();
            if (jsonNode.has("uc_pk"))
                listing.setUcPKL(jsonNode.get("uc_pk").textValue());
            if (jsonNode.has("uc_update_ts"))
                listing.setUcUpdateTSL(jsonNode.get("uc_update_ts").textValue());
            if (jsonNode.has("uc_version"))
                listing.setUcVersionL(jsonNode.get("uc_version").textValue());
            if (jsonNode.has("uc_row_type"))
                listing.setUcRowTypeL(jsonNode.get("uc_row_type").textValue());
            if (jsonNode.has("uc_type"))
                listing.setUcTypeL(jsonNode.get("uc_type").textValue());
            String dataStr = jsonNode.get("data").textValue();
            JsonNode dataNode = mapper.readTree(dataStr);
            listing.setDataL(dataStr);

            if (dataNode.has("ListAgentKeyNumeric")) {
                listing.setListAgentKeyL(dataNode.get("ListAgentKeyNumeric").textValue());
            } else {
                Integer x = 100000 + rnd.nextInt(900000);
                listing.setListAgentKeyL(x + COMPASS_NULL);
            }
            if (dataNode.has("BuyerAgentKeyNumeric")) {
                listing.setBuyerAgentKeyL(dataNode.get("BuyerAgentKeyNumeric").textValue());
            } else {
                Integer x = 100000 + rnd.nextInt(900000);
                listing.setBuyerAgentKeyL(x + COMPASS_NULL);
            }

            if (dataNode.has("CoListAgentKeyNumeric")) {
                listing.setCoListAgentKeyL(dataNode.get("CoListAgentKeyNumeric").textValue());
            } else {
                Integer x = 100000 + rnd.nextInt(900000);
                listing.setCoListAgentKeyL(x + COMPASS_NULL);
            }

            if (dataNode.has("CoBuyerAgentKeyNumeric")) {
                listing.setCoBuyerAgentKeyL(dataNode.get("CoBuyerAgentKeyNumeric").textValue());
            } else {
                Integer x = 100000 + rnd.nextInt(900000);
                listing.setCoBuyerAgentKeyL(x + COMPASS_NULL);
            }

            if (dataNode.has("ListOfficeKeyNumeric")) {
                listing.setListOfficeKeyL(dataNode.get("ListOfficeKeyNumeric").textValue());
            } else {
                Integer x = 100000 + rnd.nextInt(900000);
                listing.setListOfficeKeyL(x + COMPASS_NULL);
            }
            if (dataNode.has("BuyerOfficeKeyNumeric")) {
                listing.setBuyerOfficeKeyL(dataNode.get("BuyerOfficeKeyNumeric").textValue());
            } else {
                Integer x = 100000 + rnd.nextInt(900000);
                listing.setBuyerOfficeKeyL(x + COMPASS_NULL);
            }

            if (dataNode.has("CoListOfficeKeyNumeric")) {
                listing.setCoListOfficeKeyL(dataNode.get("CoListOfficeKeyNumeric").textValue());
            } else {
                Integer x = 100000 + rnd.nextInt(900000);
                listing.setCoListOfficeKeyL(x + COMPASS_NULL);
            }

            if (dataNode.has("CoBuyerOfficeKeyNumeric")) {
                listing.setCoBuyerOfficeKeyL(dataNode.get("CoBuyerOfficeKeyNumeric").textValue());
            } else {
                Integer x = 100000 + rnd.nextInt(900000);
                listing.setCoBuyerOfficeKeyL(x + COMPASS_NULL);
            }
            //  System.out.println("### list obj " + listing);
            return listing;
        }).name("Source: Listings");

        bsTableEnv.registerDataStream("Listings", listingStream, "ucPKL, " +
                "ucUpdateTSL, " +
                "ucVersionL, " +
                "ucRowTypeL, " +
                "ucTypeL, " +
                "listAgentKeyL, " +
                "buyerAgentKeyL, " +
                "coListAgentKeyL, " +
                "coBuyerAgentKeyL, " +
                "listOfficeKeyL, " +
                "buyerOfficeKeyL, " +
                "coListOfficeKeyL, " +
                "coBuyerOfficeKeyL, " +
                "dataL," +
                "proctime.proctime");

        Table latestListingsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY ucPKL ORDER BY proctime DESC) " +
                "AS row_num FROM Listings)" +
                "WHERE row_num = 1");
        return latestListingsTbl;
    }

    private static Table processAgents() {
        Properties properties = setProps();
        properties.setProperty("group.id", "agents-group");
        FlinkKafkaConsumer<ObjectNode> agentKafkaConsumer = new FlinkKafkaConsumer<>(agentsTopic, new JSONKeyValueDeserializationSchema(true), properties);
        agentKafkaConsumer.setStartFromEarliest();
        DataStream<Agent> agentStream = bsEnv.addSource(agentKafkaConsumer).map((MapFunction<ObjectNode, Agent>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Agent agent = new Agent();
            if (jsonNode.has("uc_pk"))
                agent.setUcPKA(jsonNode.get("uc_pk").textValue());
            if (jsonNode.has("uc_update_ts"))
                agent.setUcUpdateTSA(jsonNode.get("uc_update_ts").textValue());
            if (jsonNode.has("uc_version"))
                agent.setUcVersionA(jsonNode.get("uc_version").textValue());
            if (jsonNode.has("uc_row_type"))
                agent.setUcRowTypeA(jsonNode.get("uc_row_type").textValue());
            if (jsonNode.has("uc_type"))
                agent.setUcTypeA(jsonNode.get("uc_type").textValue());
            agent.setDataA(jsonNode.get("data").textValue());
            return agent;
        }).name("Source: Agents");

        bsTableEnv.registerDataStream("Agents", agentStream, "ucPKA, " +
                "ucUpdateTSA, " +
                "ucVersionA, " +
                "ucRowTypeA, " +
                "ucTypeA, " +
                "dataA," +
                "proctime.proctime");

        Table latestAgentsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY ucPKA ORDER BY proctime DESC) " +
                "AS row_num FROM Agents)" +
                "WHERE row_num = 1");

        return latestAgentsTbl;
    }

    private static Table processOffices() {
        Properties properties = setProps();
        properties.setProperty("group.id", "offices-group");
        FlinkKafkaConsumer<ObjectNode> officeKafkaConsumer = new FlinkKafkaConsumer<>(officesTopic, new JSONKeyValueDeserializationSchema(true), properties);
        officeKafkaConsumer.setStartFromEarliest();
        DataStream<Office> officeStream = bsEnv.addSource(officeKafkaConsumer).map((MapFunction<ObjectNode, Office>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Office office = new Office();
            if (jsonNode.has("uc_pk"))
                office.setUcPKO(jsonNode.get("uc_pk").textValue());
            if (jsonNode.has("uc_update_ts"))
                office.setUcUpdateTSO(jsonNode.get("uc_update_ts").textValue());
            if (jsonNode.has("uc_version"))
                office.setUcVersionO(jsonNode.get("uc_version").textValue());
            if (jsonNode.has("uc_row_type"))
                office.setUcRowTypeO(jsonNode.get("uc_row_type").textValue());
            if (jsonNode.has("uc_type"))
                office.setUcTypeO(jsonNode.get("uc_type").textValue());
            office.setDataO(jsonNode.get("data").textValue());
            return office;
        }).name("Source: Offices");

        bsTableEnv.registerDataStream("Offices", officeStream, "ucPKO, " +
                "ucUpdateTSO, " +
                "ucVersionO, " +
                "ucRowTypeO, " +
                "ucTypeO, " +
                "dataO," +
                "proctime.proctime");

        Table latestOfficesTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY ucPKO ORDER BY proctime DESC) " +
                "AS row_num FROM Offices)" +
                "WHERE row_num = 1");

        return latestOfficesTbl;
    }

    private static Table processOpenHouses() {
        Properties properties = setProps();
        properties.setProperty("group.id", "openhouses-group");
        FlinkKafkaConsumer<ObjectNode> openHouseKafkaConsumer = new FlinkKafkaConsumer<>(openHousesTopic, new JSONKeyValueDeserializationSchema(true), properties);
        openHouseKafkaConsumer.setStartFromEarliest();
        DataStream<OpenHouse> openHouseStream = bsEnv.addSource(openHouseKafkaConsumer).map((MapFunction<ObjectNode, OpenHouse>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            OpenHouse openHouse = new OpenHouse();
            if (jsonNode.has("uc_pk"))
                openHouse.setUcPKOH(jsonNode.get("uc_pk").textValue());
            if (jsonNode.has("uc_update_ts"))
                openHouse.setUcUpdateTSOH(jsonNode.get("uc_update_ts").textValue());
            if (jsonNode.has("uc_version"))
                openHouse.setUcVersionOH(jsonNode.get("uc_version").textValue());
            if (jsonNode.has("uc_row_type"))
                openHouse.setUcRowTypeOH(jsonNode.get("uc_row_type").textValue());
            if (jsonNode.has("uc_type"))
                openHouse.setUcTypeOH(jsonNode.get("uc_type").textValue());
            String dataStr = jsonNode.get("data").textValue();
            JsonNode dataNode = mapper.readTree(dataStr);
            openHouse.setDataOH(dataStr);
            if (dataNode.has("ListingKeyNumeric"))
                openHouse.setListingKeyOH(dataNode.get("ListingKeyNumeric").textValue());
            return openHouse;
        }).name("Source: OpenHouses");

        bsTableEnv.registerDataStream("OpenHouses", openHouseStream, "ucPKOH, " +
                "ucUpdateTSOH, " +
                "ucVersionOH, " +
                "ucRowTypeOH, " +
                "ucTypeOH, " +
                "listingKeyOH, " +
                "dataOH," +
                "proctime.proctime");

        Table latestOpenHousesTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY ucPKOH ORDER BY proctime DESC) " +
                "AS row_num FROM OpenHouses)" +
                "WHERE row_num = 1");

        return latestOpenHousesTbl;
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
         * OFFICES
         */
        Table latestOfficesTbl = processOffices();
        bsTableEnv.registerTable("latestOffices", latestOfficesTbl);
//        Table ofcResult = bsTableEnv.sqlQuery(
//                "SELECT * FROM latestOffices");
//        bsTableEnv.toRetractStream(ofcResult, Row.class).print();

        /**
         * OPEN HOUSES
         */
        Table latestOpenHousesTbl = processOpenHouses();
        bsTableEnv.registerTable("latestOpenHouses", latestOpenHousesTbl);
//        Table ohResult = bsTableEnv.sqlQuery(
//                "SELECT * FROM latestOpenHouses");
//        bsTableEnv.toRetractStream(ohResult, Row.class).print();

        /**
         * JOIN
         */
        Table joinedTbl = bsTableEnv.sqlQuery(
                "SELECT * FROM latestListings l " +
                        "LEFT JOIN latestAgents aa ON l.listAgentKeyL = aa.ucPKA " +
                        "LEFT JOIN latestAgents ab ON l.buyerAgentKeyL = ab.ucPKA " +
                        "LEFT JOIN latestAgents ac ON l.coListAgentKeyL = ac.ucPKA " +
                        "LEFT JOIN latestAgents ad ON l.coBuyerAgentKeyL = ad.ucPKA " +
                        "LEFT JOIN latestOffices oa ON l.listOfficeKeyL = oa.ucPKO " +
                        "LEFT JOIN latestOffices ob ON l.buyerOfficeKeyL = ob.ucPKO " +
                        "LEFT JOIN latestOffices oc ON l.coListOfficeKeyL = oc.ucPKO " +
                        "LEFT JOIN latestOffices od ON l.coBuyerOfficeKeyL = od.ucPKO " +
                        "LEFT JOIN latestOpenHouses oh ON l.ucPKL = oh.listingKeyOH"
        );
        //       bsTableEnv.toRetractStream(joinedTbl, Row.class).print();
        DataStream<Tuple2<Boolean, Row>> joinedStream = bsTableEnv.toRetractStream(joinedTbl, Row.class);
        joinedStream.addSink(kafkaProducer);
        bsEnv.execute("test-job");

    }

}
