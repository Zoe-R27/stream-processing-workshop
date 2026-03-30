package org.improving.workshop.exercises.stateful;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import static org.apache.kafka.streams.kstream.EmitStrategy.log;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

/**
 * Goals -
 * 1. Count the total tickets sold by each artist
 */
@Slf4j
public class ArtistTicketCount {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-artist-ticket-count";

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // hint: store Events in a table so that the ticket can reference them to find the Artist
        // see samples/PurchaseEventTicket for an example of creating a KTable

        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_EVENT_JSON)
                );
        // capture the backside of the table to log a confirmation that the Event was received
        eventsTable.toStream().peek((key, event) -> log.info("Event '{}' registered for artist '{}' at venue '{}' with a capacity of {}.", key, event.artistid(), event.venueid(), event.capacity()));


        builder
            .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
            .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))

            // solution goes here
                // rekey by eventid so we can join against the event ktable
                .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid(), Named.as("rekey-by-eventid"))

                // join the incoming ticket to the event that it is for
                .join(eventsTable, (ticket, event) -> event.artistid())
                .selectKey((eventId, artistId) -> artistId)
                .groupByKey()
                .count(
                        Named.as("artistCounts")
                )
                .toStream()
            .peek((artistId, count) -> log.info("Artist '{}' has sold {} total tickets", artistId, count))
            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    @Data
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
    }
}