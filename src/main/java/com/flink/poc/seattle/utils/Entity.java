package com.flink.poc.seattle.utils;

import com.flink.poc.seattle.entity.*;
import com.flink.poc.utils.AvroUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;

// TODO
public class Entity {

    private static ObjectMapper mapper = new ObjectMapper();

    public static Listing createListing(byte[] data) throws IOException {
        // LISTING_PRIMARY_KEY = 'LN'
        Listing listing = new Listing();
        GenericRecord message = AvroUtils.deSerializeMessageee(data);
        ByteBuffer bb = (ByteBuffer) message.get("payload");
        if (bb.hasArray()) {
            String converted = new String(bb.array(), "UTF-8");
            listing.setDataL(converted);
            JsonNode jn = mapper.readTree(converted);
            listing.setPkL(jn.get("LN").asText());
            // Agents
            if (jn.has("LAG"))
                listing.setListingAgent(jn.get("LAG").asText());
            if (jn.has("CLA"))
                listing.setCLA(jn.get("CLA").asText());
            if (jn.has("SAG"))
                listing.setSAG(jn.get("SAG").asText());
            if (jn.has("SCA"))
                listing.setSCA(jn.get("SCA").asText());
            // Offices
            if (jn.has("LO"))
                listing.setLO(jn.get("LO").asText());
            if (jn.has("COLO"))
                listing.setCOLO(jn.get("COLO").asText());
            if (jn.has("SO"))
                listing.setSO(jn.get("SO").asText());
            if (jn.has("SCO"))
                listing.setSCO(jn.get("SCO").asText());
        }
        return listing;
    }

    public static Rental createRental(byte[] data) throws IOException {
        // LISTING_PRIMARY_KEY = 'LN'
        Rental listing = new Rental();
        GenericRecord message = AvroUtils.deSerializeMessageee(data);
        ByteBuffer bb = (ByteBuffer) message.get("payload");
        if (bb.hasArray()) {
            String converted = new String(bb.array(), "UTF-8");
            listing.setDataL(converted);
            JsonNode jn = mapper.readTree(converted);
            listing.setPkL(jn.get("LN").asText());
            // Agents
            if (jn.has("LAG"))
                listing.setListingAgent(jn.get("LAG").asText());
            if (jn.has("CLA"))
                listing.setCLA(jn.get("CLA").asText());
            if (jn.has("SAG"))
                listing.setSAG(jn.get("SAG").asText());
            if (jn.has("SCA"))
                listing.setSCA(jn.get("SCA").asText());
            // Offices
            if (jn.has("LO"))
                listing.setLO(jn.get("LO").asText());
            if (jn.has("COLO"))
                listing.setCOLO(jn.get("COLO").asText());
            if (jn.has("SO"))
                listing.setSO(jn.get("SO").asText());
            if (jn.has("SCO"))
                listing.setSCO(jn.get("SCO").asText());
        }
        return listing;
    }


    public static Agent createAgent(byte[] data) throws IOException {
        Agent agent = new Agent();
        GenericRecord message = AvroUtils.deSerializeMessageee(data);
        ByteBuffer bb = (ByteBuffer) message.get("payload");
        if (bb.hasArray()) {
            String converted = new String(bb.array(), "UTF-8");
            agent.setDataA(converted);
            JsonNode jn = mapper.readTree(converted);
            agent.setPkA(jn.get("MemberMLSID").asText());
        }
        return agent;
    }

    public static Office createOffice(byte[] data) throws IOException {
        Office office = new Office();
        GenericRecord message = AvroUtils.deSerializeMessageee(data);
        ByteBuffer bb = (ByteBuffer) message.get("payload");
        if (bb.hasArray()) {
            String converted = new String(bb.array(), "UTF-8");
            office.setDataO(converted);
            JsonNode jn = mapper.readTree(converted);
            office.setPkO(jn.get("OfficeMLSID").asText());
        }
        return office;
    }

    public static OpenHouse createOpenHouse(byte[] data) throws IOException {
        OpenHouse openHouse = new OpenHouse();
        GenericRecord message = AvroUtils.deSerializeMessageee(data);
        ByteBuffer bb = (ByteBuffer) message.get("payload");
        if (bb.hasArray()) {
            String converted = new String(bb.array(), "UTF-8");
            openHouse.setDataOH(converted);
            JsonNode jn = mapper.readTree(converted);
            openHouse.setPkOH(jn.get("LN").asText());
            openHouse.setListingKeyOH(jn.get("LN").asText());
        }
        return openHouse;
    }


}
