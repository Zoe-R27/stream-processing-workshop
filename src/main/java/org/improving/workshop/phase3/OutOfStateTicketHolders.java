package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

import static org.improving.workshop.Streams.TOPIC_DATA_DEMO_STREAMS;
import static org.improving.workshop.Streams.startStreams;

public class OutOfStateTicketHolders {
    public static final String OUTPUT_TOPIC = "kafka-workshop-phase3-out-of-state-ticket-holders";
    public static final String VENUE_ADDRESS_TOPIC = "kafka-workshop-phas3-venue-addresses";
    public static final String CUSTOMER_ADDRESS_TOPIC = "kafka-workshop-phas3-customer-addresses";

    public static final JsonSerde<CustomerAddressTicket> CUSTOMER_ADDRESS_TICKET_JSON_SERDE = new JsonSerde<>(CustomerAddressTicket.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        configureTopology(builder);

        startStreams(builder);
    }

    // https://excalidraw.com/#room=04c461b0eded34d8be3a,-qc3vkyxs7ptYEqDMO6s8w

    static void configureTopology(final StreamsBuilder builder) {

        var addresses = builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with (Serdes.String(), SERDE_ADDRESS_JSON))
                .peek((streamId, stream) -> log.info("Stream Recieved: {}", stream));

        var customerAddresses = builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with (Serdes.String(), SERDE_ADDRESS_JSON))
                .peek((streamId, stream) -> log.info("Stream Recieved: {}", stream))
                // may need to filter out addresses with a null customerid
                .selectKey((addressId, address) -> address.customerid());

        var venueStates = builder
                .stream(TOPIC_DATA_DEMO_VENUES, Consumed.with (Serdes.String(), SERDE_ADDRESS_JSON))
                .peek((streamId, stream) -> log.info("Stream Recieved: {}", stream));

        builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with (Serdes.String(), SERDE_ADDRESS_JSON));


    }

    @Data
    @AllArgsConstructor
    public static class CustomerAddressTicket {
        private String customerId;
        private Address customerAddress;
        private Ticket ticket;
    }

}