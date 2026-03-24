package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.msse.demo.mockdata.music.event.Event;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

import static org.improving.workshop.Streams.startStreams;

@Slf4j
public class OutOfStateTicketHolders {
    public static final String OUTPUT_TOPIC = "kafka-workshop-phase3-out-of-state-ticket-holders";

    public static final JsonSerde<CustomerAddressTicket> CUSTOMER_ADDRESS_TICKET_JSON_SERDE = new JsonSerde<>(CustomerAddressTicket.class);
    public static final JsonSerde<VenueAddressPair> VENUE_ADDRESS_JSON_SERDE = new JsonSerde<>(VenueAddressPair.class);
    public static final JsonSerde<EventVenueAddress> EVENT_VENUE_ADDRESS_JSON_SERDE = new JsonSerde<>(EventVenueAddress.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        configureTopology(builder);

        startStreams(builder);
    }

    // https://excalidraw.com/#room=04c461b0eded34d8be3a,-qc3vkyxs7ptYEqDMO6s8w

    static void configureTopology(final StreamsBuilder builder) {
        // KTable used for event venue address lookup, gets merged with venue
        KTable<String, Address> addressKTable = builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with (Serdes.String(), SERDE_ADDRESS_JSON))
                .peek((streamId, stream) -> log.info("Address Stream consumed for venues Received: {}", stream))
                .peek((streamId, stream) -> log.info("addresses Storing address: {}", stream))
                .toTable(
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("addresses"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON)
                );

        // KTable used for customer address lookup, gets merged with tickets
        KTable<String, Address> customerAddressTable = builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with (Serdes.String(), SERDE_ADDRESS_JSON))
                .peek((streamId, stream) -> log.info("Address Stream consumed for customers Received: {}", stream))
                // may need to filter out addresses with a null customerid
                .selectKey((addressId, address) -> address.customerid(), Named.as("rekey-address-by-customerid"))
                .peek((streamId, stream) -> log.info("customerAddresses Storing address: {}", stream))
                .toTable(
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("customerAddresses"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON)
                );

        // KTable used for Venue Address Lookup, gets merged with events
        KTable<String, VenueAddressPair> venueAddressTable = builder
                .stream(TOPIC_DATA_DEMO_VENUES, Consumed.with (Serdes.String(), SERDE_VENUE_JSON))
                .peek((streamId, stream) -> log.info("Venue Stream Received: {}", stream))
                .selectKey((k, venue) -> venue.addressid(), Named.as("rekey-venue-by-venue.addressid"))
                .join(
                        addressKTable,
                        (addressId, venue, address) -> new VenueAddressPair(venue, address)
                )
                .selectKey((addressId, venueAddress) -> venueAddress.venue.id(), Named.as("rekey-venue-address-by-venue-id"))
                .peek((streamId, stream) -> log.info("venueAddresses Storing VenueAddress: {}", stream))
                .toTable(
                        Materialized
                                .<String, VenueAddressPair>as(persistentKeyValueStore("venueAddresses"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(VENUE_ADDRESS_JSON_SERDE)
                );

        // KTable used for Event Address Lookup, Merged with Customer Tickets
        KTable<String, EventVenueAddress> eventAddressTable = builder
                .stream(TOPIC_DATA_DEMO_EVENTS, Consumed.with (Serdes.String(), SERDE_EVENT_JSON))
                .peek((streamId, stream) -> log.info("Event Stream Received: {}", stream))
                .selectKey((id, event) -> event.venueid(), Named.as("rekey-event-by-venue-id"))
                .peek((k, v) -> log.info("joining event: {}", v))
                .join(
                        venueAddressTable,
                        (venueId, event, venueAddress) -> new EventVenueAddress(event, venueAddress.venue, venueAddress.address)
                )
                .peek((k,v) -> log.info("event venue: {}", v))
                .selectKey((venue_id, eventVenueAddress) -> eventVenueAddress.event.id(), Named.as("rekey-event-venue-address-by-event-id"))
                .peek((streamId, stream) -> log.info("eventVenueAddresses Storing address: {}", stream))
                .toTable(
                        Materialized
                                .<String, EventVenueAddress>as(persistentKeyValueStore("eventVenueAddresses"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(EVENT_VENUE_ADDRESS_JSON_SERDE)
                );

        // When a ticket is purchased, find customer address, find event venue address
        // find tickets where the customer lives in a different state
        // this can be used in addition to streams to determine good future event locations
        builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with (Serdes.String(), SERDE_TICKET_JSON))
                .peek((streamId, stream) -> log.info("Ticket Stream Received: {}", stream))
                .selectKey((k, ticket) -> ticket.customerid(), Named.as("rekey-ticket-by-customerid"))
                .join(
                        customerAddressTable,
                        (customer_id, ticket, customerAddress) -> new TicketCustomerAddress(ticket, customerAddress)
                )
                .peek((streamId, stream) -> log.info("Found customer ticket address: {}", stream))
                .selectKey((customer_id, ticketCustomerAddress) -> ticketCustomerAddress.ticket.eventid(), Named.as("rekey-ticket-customer-address-by-event-id"))
                .join(
                        eventAddressTable,
                        (event_id, ticketCustomer, event) ->
                                new TicketCustomerEvent(ticketCustomer.ticket, ticketCustomer.customerAddress, event.address, event.event, event.venue)
                )
                .peek((k, v) -> log.info("from {} to {} customer ticket address and venue address: {}", v.customerAddress.state(), v.eventAddress.state(), v))
                // if we care about instate customers, we could branch, but that will be a "hot topic"
                .filter((customer_id, ticketCustomerEvent) ->
                        // different states == out of state event
                        !ticketCustomerEvent.customerAddress.state().equals(ticketCustomerEvent.eventAddress.state())
                )
                .peek((k,v) -> log.info("Customer from {} going to {} event {} customer {}", v.customerAddress.state(), v.eventAddress.state(), v.customerAddress.customerid(), v.event.id()))
                .mapValues((k,v) -> new CustomerAddressTicket(v.ticket.customerid(), v.customerAddress, v.ticket))
                // potentially want to repartition by customer
                // .selectKey()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CUSTOMER_ADDRESS_TICKET_JSON_SERDE));




    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TicketCustomerEvent {
        private Ticket ticket;
        private Address customerAddress;
        private Address eventAddress;
        private Event event;
        private Venue venue;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TicketCustomerAddress {
        private Ticket ticket;
        private Address customerAddress;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EventVenueAddress {
        private Event event;
        private Venue venue;
        private Address address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VenueAddressPair {
        private Venue venue;
        private Address address;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerAddressTicket {
        private String customerId;
        private Address customerAddress;
        private Ticket ticket;
    }

}