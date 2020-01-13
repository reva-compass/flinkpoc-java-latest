package com.flink.poc.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;


public class TestBlink {

    private static StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    private static EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    private static StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

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
                new FlinkKafkaConsumer<>("poc_test_listing", new JSONKeyValueDeserializationSchema(true), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<Listing> listingStream = bsEnv.addSource(kafkaConsumer).map((MapFunction<ObjectNode, Listing>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Listing listing = new Listing();
            listing.setListingIdL(jsonNode.get("Listing ID").textValue());
            listing.setEarnestPayableToL(jsonNode.get("Earnest $ Payable To").textValue());
            listing.setStatusChangeDateL(jsonNode.get("Status Change Date").textValue());
            listing.setInclusionsL(jsonNode.get("Inclusions").textValue());
            listing.setCountyL(jsonNode.get("County").textValue());
            if (jsonNode.has("Agent ID"))
                listing.setAgentIdL(jsonNode.get("Agent ID").textValue());
            listing.setTermsOfferedL(jsonNode.get("Terms Offered").textValue());
            listing.setNbrOfAcresL(jsonNode.get("Nbr of Acres").textValue());
            listing.setCoListingMemberUrlL(jsonNode.get("CoListingMemberUrl").textValue());
            if (jsonNode.has("CoList Agent ID"))
                listing.setCoListAgentIdL(jsonNode.get("CoList Agent ID").textValue());
            if (jsonNode.has("Buyer Agent ID"))
                listing.setBuyerAgentIdL(jsonNode.get("Buyer Agent ID").textValue());
            if (jsonNode.has("CoBuyer Agent ID"))
                listing.setCoBuyerAgentIdL(jsonNode.get("CoBuyer Agent ID").textValue());
            listing.setListOfficeBoardCodeL(jsonNode.get("List Office Board Code").textValue());
            listing.setList207L(jsonNode.get("LIST_207").textValue());
            System.out.println("### list obj " + listing);
            return listing;
        });

        bsTableEnv.registerDataStream("Listings", listingStream, "listingIdL, " +
                "agentIdL, " +
                "buyerAgentIdL, " +
                "coListAgentIdL, " +
                "coBuyerAgentIdL, " +
                "earnestPayableToL, " +
                "statusChangeDateL, " +
                "inclusionsL, " +
                "countyL, " +
                "termsOfferedL, " +
                "nbrOfAcresL, " +
                "coListingMemberUrlL, " +
                "listOfficeBoardCodeL, " +
                "list207L," +
                "proctime.proctime");
//        Table result2 = bsTableEnv.sqlQuery(
//                "SELECT * FROM Listings");
//        bsTableEnv.toAppendStream(result2, Row.class).print();

        Table latestListingsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY listingIdL ORDER BY proctime DESC) " +
                "AS row_num FROM Listings)" +
                "WHERE row_num = 1");
        return latestListingsTbl;
    }

    private static Table processAgents() {
        Properties properties = setProps();
        properties.setProperty("group.id", "agents-group");
        FlinkKafkaConsumer<ObjectNode> agentKafkaConsumer = new FlinkKafkaConsumer<>("poc_test_agent", new JSONKeyValueDeserializationSchema(true), properties);
        agentKafkaConsumer.setStartFromEarliest();
        DataStream<Agent> agentStream = bsEnv.addSource(agentKafkaConsumer).map((MapFunction<ObjectNode, Agent>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Agent agent = new Agent();
            agent.setAgentIdA(jsonNode.get("Agent ID").textValue());
            agent.setCityA(jsonNode.get("City").textValue());
            agent.setOfficeIdA(jsonNode.get("Office ID").textValue());
            agent.setEmailA(jsonNode.get("Email").textValue());
            agent.setRenegotiationExpA(jsonNode.get("RENegotiation Exp").textValue());
            agent.setNrdsidA(jsonNode.get("NRDSID").textValue());
            agent.setMlsStatusA(jsonNode.get("MLS Status").textValue());
            agent.setAgentTimestampA(jsonNode.get("agent_timestamp").textValue());
            return agent;
        });

        bsTableEnv.registerDataStream("Agents", agentStream, "agentIdA, " +
                "cityA, " +
                "officeIdA, " +
                "emailA, " +
                "renegotiationExpA, " +
                "nrdsidA, " +
                "mlsStatusA, " +
                "agentTimestampA, " +
                "proctime.proctime");

        Table latestAgentsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY agentIdA ORDER BY proctime DESC) " +
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
//        //   bsTableEnv.toRetractStream(result4, Row.class).print();

        /**
         * JOIN
         */
        Table joinedTbl = bsTableEnv.sqlQuery(
                "SELECT * FROM latestListings l " +
                        "LEFT JOIN latestAgents aa ON l.agentIdL = aa.agentIdA " +
                        "LEFT JOIN latestAgents ab ON l.coListAgentIdL = ab.agentIdA " +
                        "LEFT JOIN latestAgents ac ON l.buyerAgentIdL = ac.agentIdA " +
                        "LEFT JOIN latestAgents ad ON l.coBuyerAgentIdL = ad.agentIdA"
        );
        bsTableEnv.toRetractStream(joinedTbl, Row.class).print();

        bsEnv.execute("test-job");

    }


}
