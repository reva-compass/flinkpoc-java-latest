package com.flink.poc.test;

import com.flink.poc.test.Agent;
import com.flink.poc.test.Listing;
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
        FlinkKafkaConsumer<ObjectNode> kafkaConsumer =
                new FlinkKafkaConsumer<>("poc_test_listing", new JSONKeyValueDeserializationSchema(true), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<Listing> listingStream = bsEnv.addSource(kafkaConsumer).map((MapFunction<ObjectNode, Listing>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Listing listing = new Listing();
            listing.setListingId(jsonNode.get("Listing ID").textValue());
            listing.setEarnestPayableTo(jsonNode.get("Earnest $ Payable To").textValue());
            listing.setStatusChangeDate(jsonNode.get("Status Change Date").textValue());
            listing.setInclusions(jsonNode.get("Inclusions").textValue());
            listing.setCounty(jsonNode.get("County").textValue());
            if (jsonNode.has("Agent ID"))
                listing.setAgentId(jsonNode.get("Agent ID").textValue());
            listing.setTermsOffered(jsonNode.get("Terms Offered").textValue());
            listing.setNbrOfAcres(jsonNode.get("Nbr of Acres").textValue());
            listing.setCoListingMemberUrl(jsonNode.get("CoListingMemberUrl").textValue());
            if (jsonNode.has("CoList Agent ID"))
                listing.setCoListAgentId(jsonNode.get("CoList Agent ID").textValue());
            if (jsonNode.has("Buyer Agent ID"))
                listing.setBuyerAgentId(jsonNode.get("Buyer Agent ID").textValue());
            if (jsonNode.has("CoBuyer Agent ID"))
                listing.setCoBuyerAgentId(jsonNode.get("CoBuyer Agent ID").textValue());
            listing.setListOfficeBoardCode(jsonNode.get("List Office Board Code").textValue());
            listing.setList207(jsonNode.get("LIST_207").textValue());
            // System.out.println("### list obj " + listing);
            return listing;
        });

        bsTableEnv.registerDataStream("Listings", listingStream, "listingId, " +
                "earnestPayableTo, " +
                "statusChangeDate, " +
                "inclusions, " +
                "county, " +
                "agentId, " +
                "termsOffered, " +
                "nbrOfAcres, " +
                "coListingMemberUrl, " +
                "coListAgentId, " +
                "buyerAgentId, " +
                "coBuyerAgentId, " +
                "listOfficeBoardCode, " +
                "list207," +
                "proctime.proctime");
//        Table result2 = bsTableEnv.sqlQuery(
//                "SELECT * FROM Listings");
//        bsTableEnv.toAppendStream(result2, Row.class).print();

        Table latestListingsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY listingId ORDER BY proctime DESC) " +
                "AS row_num FROM Listings)" +
                "WHERE row_num = 1");
        return latestListingsTbl;
    }

    private static Table processAgents() {
        Properties properties = setProps();
        FlinkKafkaConsumer<ObjectNode> agentKafkaConsumer = new FlinkKafkaConsumer<>("poc_test_agent", new JSONKeyValueDeserializationSchema(true), properties);
        agentKafkaConsumer.setStartFromEarliest();
        DataStream<Agent> agentStream = bsEnv.addSource(agentKafkaConsumer).map((MapFunction<ObjectNode, Agent>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Agent agent = new Agent();
            agent.setAgentId(jsonNode.get("Agent ID").textValue());
            agent.setCity(jsonNode.get("City").textValue());
            agent.setOfficeId(jsonNode.get("Office ID").textValue());
            agent.setEmail(jsonNode.get("Email").textValue());
            agent.setRenegotiationExp(jsonNode.get("RENegotiation Exp").textValue());
            agent.setNrdsid(jsonNode.get("NRDSID").textValue());
            agent.setMlsStatus(jsonNode.get("MLS Status").textValue());
            agent.setAgentTimestamp(jsonNode.get("agent_timestamp").textValue());
            return agent;
        });

        bsTableEnv.registerDataStream("Agents", agentStream, "agentId, " +
                "city, " +
                "officeId, " +
                "email, " +
                "renegotiationExp, " +
                "nrdsid, " +
                "mlsStatus, " +
                "agentTimestamp, " +
                "proctime.proctime");

        Table latestAgentsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY agentId ORDER BY proctime DESC) " +
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
        Table result3 = bsTableEnv.sqlQuery(
                "SELECT * FROM latestListings");
        //   bsTableEnv.toRetractStream(result3, Row.class).print();

        /**
         * AGENTS
         */
        Table latestAgentsTbl = processAgents();
        bsTableEnv.registerTable("latestAgents", latestAgentsTbl);
        Table result4 = bsTableEnv.sqlQuery(
                "SELECT * FROM latestAgents");
        //   bsTableEnv.toRetractStream(result4, Row.class).print();

        /**
         * JOIN
         */
        Table joinedTbl = bsTableEnv.sqlQuery(
                "SELECT * FROM latestListings l " +
                        "LEFT JOIN latestAgents aa ON l.agentId = aa.agentId " +
                        "LEFT JOIN latestAgents ab ON l.coListAgentId = ab.agentId " +
                        "LEFT JOIN latestAgents ac ON l.buyerAgentId = ac.agentId " +
                        "LEFT JOIN latestAgents ad ON l.coBuyerAgentId = ad.agentId"
        );
        bsTableEnv.toRetractStream(joinedTbl, Row.class).print();

        bsEnv.execute("test-job");

    }


}
