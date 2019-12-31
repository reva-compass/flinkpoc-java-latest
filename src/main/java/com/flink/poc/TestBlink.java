package com.flink.poc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

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

    private static void processListings() {
        Properties properties = setProps();
        FlinkKafkaConsumer<ObjectNode> kafkaConsumer =
                new FlinkKafkaConsumer<>("poc_test_listing", new JSONKeyValueDeserializationSchema(true), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStream<Listing> stream = bsEnv.addSource(kafkaConsumer).map((MapFunction<ObjectNode, Listing>) jsonNodes -> {
            JsonNode jsonNode = jsonNodes.get("value");
            Listing listing = new Listing();
            listing.setListingId(jsonNode.get("Listing ID").textValue());
            listing.setEarnestPayableTo(jsonNode.get("Earnest $ Payable To").textValue());
            listing.setStatusChangeDate(jsonNode.get("Status Change Date").textValue());
            listing.setInclusions(jsonNode.get("Inclusions").textValue());
            listing.setCounty(jsonNode.get("County").textValue());
            listing.setAgentId(jsonNode.get("Agent ID").textValue());
            listing.setTermsOffered(jsonNode.get("Terms Offered").textValue());
            listing.setNbrOfAcres(jsonNode.get("Nbr of Acres").textValue());
            listing.setCoListingMemberUrl(jsonNode.get("CoListingMemberUrl").textValue());
            listing.setCoListAgentId(jsonNode.get("CoList Agent ID").textValue());
            listing.setListOfficeBoardCode(jsonNode.get("List Office Board Code").textValue());
            listing.setList207(jsonNode.get("LIST_207").textValue());
           // System.out.println("### list obj " + listing);
            return listing;
        });

        bsTableEnv.registerDataStream("Orders", stream, "listingId, " +
                "earnestPayableTo, " +
                "statusChangeDate, " +
                "inclusions, " +
                "county, " +
                "agentId, " +
                "termsOffered, " +
                "nbrOfAcres, " +
                "coListingMemberUrl, " +
                "coListAgentId, " +
                "listOfficeBoardCode, " +
                "list207");
        Table result2 = bsTableEnv.sqlQuery(
                "SELECT * FROM Orders");
        bsTableEnv.toAppendStream(result2, Listing.class).print();

    }

    public static void main(String[] args) throws Exception {
       processListings();
        System.out.println("### inside main");
        bsEnv.execute("test-job");

    }


}
