package org.improving.workshop.phase3;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;
import java.util.*;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

@SuppressWarnings("InfiniteLoopStatement")
public class LeastStreamingTicketHolders {
    public static final JsonSerde<Event> SERDE_EVENT_JSON = new JsonSerde<>(Event.class);
    public static final JsonSerde<Ticket> SERDE_TICKET_JSON = new JsonSerde<>(Ticket.class);
    public static final JsonSerde<EventTicket> SERDE_EVENT_TICKET_JSON = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<Stream> SERDE_STREAM_JSON = new JsonSerde<>(Stream.class);
    public static final int bottomStreamers = 10;
    public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);
    public static final String LOWEST_STREAMED_TICKETED_CUSTOMERS = "lowest-streamed-ticketed-customers-output-topic";
    // Jackson is converting Value into Integer Not Long due to erasure,
    //public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE = new JsonSerde<>(LinkedHashMap.class);
    public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE
            = new JsonSerde<>(
            new TypeReference<LinkedHashMap<String, Long>>() {
            },
            new ObjectMapper()
                    .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
    );
    public static void main(String[] args) {
    }

    static void leastStreamingTicketHoldersTopology(final StreamsBuilder builder) {
        // store events in a table so that the ticket can reference them to find capacity
        KTable<String, Event> eventsTable = builder
                .table(
                        "data-demo-events",
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_JSON)
                );

        // Get all the tickets for an event and join with the events on the event Id
        KStream<String, EventTicket> eventTicketByCustomerId = builder
                .stream("data-demo-tickets", Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                // rekey by eventid so we can join against the event ktable
                .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid(), Named.as("rekey-by-eventid"))

                // join the incoming ticket to the event that it is for
                .join(
                        eventsTable,
                        (eventId, ticket, event) -> new EventTicket(ticket, event)
                )
                .selectKey((eventId, event) -> event.ticket.customerid(), Named.as("rekey-by-customerid"));

        // KTable to store the Event Tickets - Key on Customer Id
        KTable<String, EventTicket> eventTicketByCustomerTable = builder
                .table(
                        eventTicketByCustomerId.toString(),
                        Materialized
                                .<String, EventTicket>as(persistentKeyValueStore("eventTicket"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_TICKET_JSON)
                );

        // Join the Event Tickets with the Streams on the customer Id, filter where the stream and the event have the same artist Id
        //
        builder
                .stream("data-demo-streams", Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .selectKey((streamId, streamRequest) -> streamRequest.customerid(), Named.as("rekey-stream-by-customerid"))
                .join(
                        eventTicketByCustomerTable,
                        (customerid, stream, eventTicket) -> new CustomerStreamEventTicket(stream, eventTicket)
                )
                .filter(((s, customerStreamEventTicket) -> Objects.equals(customerStreamEventTicket.eventTicket.event.artistid(), customerStreamEventTicket.stream.artistid())))
                .groupByKey()
                .aggregate(
                        SortedCounterMap::new,

                        // aggregator
                        (customerId, customerStream, customerArtistStreamCounts) -> {
                            customerArtistStreamCounts.incrementCount(customerStream.stream.artistid());
                            return customerArtistStreamCounts;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, SortedCounterMap>as(persistentKeyValueStore("ticketed-customer-artist-stream-counts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(COUNTER_MAP_JSON_SERDE)
                )
                .toStream()
                .mapValues(sortedCounterMap -> sortedCounterMap.top(bottomStreamers))
                .to(LOWEST_STREAMED_TICKETED_CUSTOMERS, Produced.with(Serdes.String(), LINKED_HASH_MAP_JSON_SERDE));
    }

    @Data
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;

        public EventTicket(Ticket ticket, Event event) {
            this.ticket = ticket;
            this.event = event;
        }
    }

    @Data
    @AllArgsConstructor
    public static class CustomerStreamEventTicket {
        private Stream stream;
        private EventTicket eventTicket;

        public CustomerStreamEventTicket(Stream stream, EventTicket eventTicket) {
            this.stream = stream;
            this.eventTicket = eventTicket;
        }
    }

    @Data
    @AllArgsConstructor
    public static class SortedCounterMap {
        private int maxSize;
        private LinkedHashMap<String, Long> map;

        public SortedCounterMap() {
            this(1000);
        }

        public SortedCounterMap(int maxSize) {
            this.maxSize = maxSize;
            this.map = new LinkedHashMap<>();
        }

        public void incrementCount(String id) {
            map.compute(id, (k, v) -> v == null ? 1 : v + 1);

            // replace with sorted map
            this.map = map.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    // keep a limit on the map size
                    .limit(maxSize)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        /**
         * Return the top {limit} items from the counter map
         *
         * @param limit the number of records to include in the returned map
         * @return a new LinkedHashMap with only the top {limit} elements
         */
        public LinkedHashMap<String, Long> top(int limit) {
            return map.entrySet().stream()
                    .limit(limit)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }
}