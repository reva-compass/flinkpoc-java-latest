package com.flink.poc.seattle;

import com.flink.poc.crmls.JoinedSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;


public class SeattleJoiner {

    private static StreamExecutionEnvironment bsEnv;
    private static ObjectMapper mapper = new ObjectMapper();

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
    private static FlinkKafkaProducer<Tuple2<Boolean, Row>> kafkaProducer =
            new FlinkKafkaProducer("test-topic", new JoinedSerializer("test-topic"), setProps(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    private static Random rnd = new Random();

    /*
data_listings_mirrored_seattle_nwmls_farmranch
data_listings_mirrored_seattle_nwmls_manufactured
data_listings_mirrored_seattle_nwmls_multifamily
data_listings_mirrored_seattle_nwmls_rental
data_listings_mirrored_seattle_nwmls_school
     */
    /* TOPICS */
    private static String condosTopic = "data_listings_mirrored_seattle_nwmls_condominium";
    private static String vacantLandTopic = "data_listings_mirrored_seattle_nwmls_vacantland";
    private static String residentialTopic = "data_listings_mirrored_seattle_nwmls_residential";
    private static String farmranchTopic = "data_listings_mirrored_seattle_nwmls_farmranch";
    private static String manufacturedTopic = "data_listings_mirrored_seattle_nwmls_manufactured";
    private static String multifamilyTopic = "data_listings_mirrored_seattle_nwmls_multifamily";
    private static String rentalTopic = "data_listings_mirrored_seattle_nwmls_rental";
    private static String agentsTopic = "data_listings_mirrored_seattle_nwmls_agent";
    private static String officesTopic = "data_listings_mirrored_seattle_nwmls_office";
    private static String openHousesTopic = "data_listings_mirrored_seattle_nwmls_openhouses";

    /* CONSUMER GROUPS */
    private static String condosConsumerGroup = "condos-group";
    private static String vacantLandConsumerGroup = "vacant-land-group";
    private static String residentialConsumerGroup = "residential-group";
    private static String farmranchConsumerGroup = "farmranch-group";
    private static String manufacturedConsumerGroup = "manufactured-group";
    private static String multifamilyConsumerGroup = "multifamily-group";
    private static String rentalConsumerGroup = "rental-group";
    private static String agentsConsumerGroup = "agents-group";
    private static String officesConsumerGroup = "offices-group";
    private static String openHousesConsumerGroup = "open-houses-group";

    private static String COMPASS_NULL = "-COMPASS-NULL";

    private static Properties setProps() {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "b-2.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-1.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-3.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092");
        return properties;
    }

    private static Table processListings(String topic, String consumerGroup, String tableName) {
        Properties properties = setProps();
        properties.setProperty("group.id", consumerGroup);
        FlinkKafkaConsumer<byte[]> listingKafkaConsumer = new FlinkKafkaConsumer<>(topic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        // TODO comment below line so you dont read from beginning
        listingKafkaConsumer.setStartFromEarliest();
        DataStream<Listing> listingsStream = bsEnv.addSource(listingKafkaConsumer).map((MapFunction<byte[], Listing>) data -> {
            return Entity.createListing(data);
        });

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

    private static Table processAgents() {
        Properties properties = setProps();
        properties.setProperty("group.id", agentsConsumerGroup);
        FlinkKafkaConsumer<byte[]> agentKafkaConsumer = new FlinkKafkaConsumer<>(agentsTopic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        // TODO comment below line so you dont read from beginning
        agentKafkaConsumer.setStartFromEarliest();
        DataStream<Agent> agentStream = bsEnv.addSource(agentKafkaConsumer).map((MapFunction<byte[], Agent>) data -> {
            return Entity.createAgent(data);
        }).name("Source: Agents");

        bsTableEnv.registerDataStream("Agents", agentStream, "pkA," +
                "dataA," +
                "proctime.proctime");

        Table latestAgentsTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY pkA ORDER BY proctime DESC) " +
                "AS row_num FROM Agents)" +
                "WHERE row_num = 1");

        return latestAgentsTbl;
    }

    private static Table processOffices() {
        Properties properties = setProps();
        properties.setProperty("group.id", officesConsumerGroup);
        FlinkKafkaConsumer<byte[]> officeKafkaConsumer = new FlinkKafkaConsumer<>(officesTopic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        // TODO comment below line so you dont read from beginning
        officeKafkaConsumer.setStartFromEarliest();
        DataStream<Office> officesStream = bsEnv.addSource(officeKafkaConsumer).map((MapFunction<byte[], Office>) data -> {
            return Entity.createOffice(data);
        }).name("Source: Offices");

        bsTableEnv.registerDataStream("Offices", officesStream, "pkO, " +
                "dataO, " +
                "proctime.proctime");

        Table latestOfficesTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY pkO ORDER BY proctime DESC) " +
                "AS row_num FROM Offices)" +
                "WHERE row_num = 1");

        return latestOfficesTbl;
    }

    private static Table processOpenHouses() {
        Properties properties = setProps();
        properties.setProperty("group.id", openHousesConsumerGroup);
        FlinkKafkaConsumer<byte[]> ohKafkaConsumer = new FlinkKafkaConsumer<>(openHousesTopic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        // TODO comment below line so you dont read from beginning
        ohKafkaConsumer.setStartFromEarliest();
        DataStream<OpenHouse> ohStream = bsEnv.addSource(ohKafkaConsumer).map((MapFunction<byte[], OpenHouse>) data -> {
            return Entity.createOpenHouse(data);
        }).name("Source: OpenHouses");

        bsTableEnv.registerDataStream("OpenHouses", ohStream, "pkOH, " +
                "listingKeyOH, " +
                "dataOH, " +
                "proctime.proctime");

        Table latestOpenHousesTbl = bsTableEnv.sqlQuery("SELECT * FROM (" +
                "SELECT *, ROW_NUMBER() " +
                "OVER (PARTITION BY pkOH ORDER BY proctime DESC) " +
                "AS row_num FROM OpenHouses)" +
                "WHERE row_num = 1");

        return latestOpenHousesTbl;
    }


    public static void main(String[] args) throws Exception {
        System.out.println("### inside main");

        /**
         * CONDOS
         */
        Table latestCondosTbl = processListings(condosTopic, condosConsumerGroup, "Condos");
        bsTableEnv.registerTable("latestCondos", latestCondosTbl);
//        Table condosRes = bsTableEnv.sqlQuery("SELECT * FROM latestCondos");
//        bsTableEnv.toRetractStream(condosRes, Row.class).print();

        /**
         * VACANT LAND
         */
        Table latestVacantLandTbl = processListings(vacantLandTopic, vacantLandConsumerGroup, "VacantLand");
        bsTableEnv.registerTable("latestVacantLand", latestVacantLandTbl);
//        Table vlRes = bsTableEnv.sqlQuery("SELECT * FROM latestVacantLand");
//        bsTableEnv.toRetractStream(vlRes, Row.class).print();

        /**
         * RESIDENTIAL
         */
        Table latestResidentialTbl = processListings(residentialTopic, residentialConsumerGroup, "Residential");
        bsTableEnv.registerTable("latestResidential", latestResidentialTbl);
//        Table resnRes = bsTableEnv.sqlQuery("SELECT * FROM latestResidential");
//        bsTableEnv.toRetractStream(resnRes, Row.class).print();

        /**
         * farmranch
         */
//        Table latestFarmranchTbl = processListings(farmranchTopic, farmranchConsumerGroup, "Farmranch");
//        bsTableEnv.registerTable("latestFarmranch", latestFarmranchTbl);
//        Table farmRes = bsTableEnv.sqlQuery("SELECT * FROM latestFarmranch");
//        bsTableEnv.toRetractStream(farmRes, Row.class).print();

        /**
         * manufactured
         */
//        Table latestManufacturedTbl = processListings(manufacturedTopic, manufacturedConsumerGroup, "Manufactured");
//        bsTableEnv.registerTable("latestManufactured", latestManufacturedTbl);
//        Table manufacturedRes = bsTableEnv.sqlQuery("SELECT * FROM latestManufactured");
//        bsTableEnv.toRetractStream(manufacturedRes, Row.class).print();

        /**
         * multifamily
         */
//        Table latestMultifamilyTbl = processListings(multifamilyTopic, multifamilyConsumerGroup, "Multifamily");
//        bsTableEnv.registerTable("latestMultifamily", latestMultifamilyTbl);
//        Table multifamilyRes = bsTableEnv.sqlQuery("SELECT * FROM latestMultifamily");
//        bsTableEnv.toRetractStream(multifamilyRes, Row.class).print();

        /**
         * rental
         */
//        Table latestRentalTbl = processListings(rentalTopic, rentalConsumerGroup, "Rental");
//        bsTableEnv.registerTable("latestRental", latestRentalTbl);
//        Table rentalRes = bsTableEnv.sqlQuery("SELECT * FROM latestRental");
//        bsTableEnv.toRetractStream(rentalRes, Row.class).print();

        /**
         * ALL LISTINGS
         */
        Table allListingsTbl = latestCondosTbl.unionAll(latestVacantLandTbl).unionAll(latestResidentialTbl);
        bsTableEnv.registerTable("allListings", allListingsTbl);

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
        Table joinedCondosTbl = bsTableEnv.sqlQuery(
                "SELECT * FROM allListings l " +
                        "LEFT JOIN latestAgents aa ON l.listingAgent = aa.pkA " +
                        "LEFT JOIN latestAgents ab ON l.CLA = ab.pkA " +
                        "LEFT JOIN latestAgents ac ON l.SAG = ac.pkA " +
                        "LEFT JOIN latestAgents ad ON l.SCA = ad.pkA " +
                        "LEFT JOIN latestOffices oa ON l.LO = oa.pkO " +
                        "LEFT JOIN latestOffices ob ON l.COLO = ob.pkO " +
                        "LEFT JOIN latestOffices oc ON l.SO = oc.pkO " +
                        "LEFT JOIN latestOffices od ON l.SCO = od.pkO " +
                        "LEFT JOIN latestOpenHouses oh ON l.pkL = oh.listingKeyOH"
        );
        bsTableEnv.toRetractStream(joinedCondosTbl, Row.class).print();
//        DataStream<Tuple2<Boolean, Row>> joinedCondosStream = bsTableEnv.toRetractStream(joinedCondosTbl, Row.class);
//        joinedCondosStream.addSink(kafkaProducer);

        bsEnv.execute("seattle-job");

    }

}
