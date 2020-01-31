package com.flink.poc.seattle.utils;

import com.flink.poc.seattle.entity.Rental;
import com.flink.poc.seattle.utils.Entity;
import com.flink.poc.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class RentalManager implements Serializable {

    private static String rentalConsumerGroup = "rental-group";
    private static String rentalTopic = "data_listings_mirrored_seattle_nwmls_rental";

    public  Table processRentals(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {
        Properties properties = Utils.setProps();
        properties.setProperty("group.id", rentalConsumerGroup);
        FlinkKafkaConsumer<byte[]> rentalsKafkaConsumer = new FlinkKafkaConsumer<>(rentalTopic, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);
        rentalsKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        // TODO comment below line so you dont read from beginning
        rentalsKafkaConsumer.setStartFromEarliest();
        DataStream<Rental> listingsStream = bsEnv.addSource(rentalsKafkaConsumer).map((MapFunction<byte[], Rental>) data -> {
            return Entity.createRental(data);
        });
        String tableName = "Rentals";
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
