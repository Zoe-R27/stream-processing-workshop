package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;
import java.util.*;
import java.util.stream.Collectors;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@SuppressWarnings("InfiniteLoopStatement")
@Slf4j
public class LeastStreamingTicketHolders {
    public static final int bottomStreamers = 2;
    public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);
    private static final JsonSerde<EventTicket> SERDE_EVENT_TICKET_JSON = new JsonSerde<>(EventTicket.class);
    private static final JsonSerde<CustomerStreamEventTicket> SERDE_CUSTOMER_STREAM_EVENT_TICKET_JSON = new JsonSerde<>(CustomerStreamEventTicket.class);
    public static final String LOWEST_STREAMED_TICKETED_CUSTOMERS_TOPIC = "kafka-workshop-lowest-streamed-ticketed-customers-output-topic";
    public static final JsonSerde<LowestStreamingCustomersPerArtist> SERDE_ARTIST_LOWEST_STREAMERS_JSON
            = new JsonSerde<>(LowestStreamingCustomersPerArtist.class);

    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        leastStreamingTicketHoldersTopology(builder, bottomStreamers);

        // fire up the engines
        startStreams(builder);
    }

    static void leastStreamingTicketHoldersTopology(final StreamsBuilder builder, int bottomNumberStreamers) {
        // store events in a table so that the ticket can reference them to find capacity
        KTable<String, Event> eventsTable = builder
                .table(
                        "data-demo-events",
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_JSON)
                );
        eventsTable.toStream().peek((s, event) -> log.info("Event {}", event));

        // Get all the tickets for an event and join with the events on the event Id
        KTable<String, EventTicket> eventTicketByCustomerIdTable = builder
                .stream("data-demo-tickets", Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                // rekey by eventid so we can join against the event ktable
                .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid(), Named.as("rekey-by-eventid"))

                // join the incoming ticket to the event that it is for
                .join(
                        eventsTable,
                        (eventId, ticket, event) -> new EventTicket(ticket, event)
                )
                // Rekey to have both customerId + artistId to make distinct for each artist the customer streams
                .selectKey((eventId, event) -> event.ticket.customerid() + "-" + event.event.artistid())
                .peek((eventTicketId, eventTicket) -> log.info("Event Ticket Received: {} for id: {}", eventTicket, eventTicketId))
                .toTable(
                        Materialized
                                .<String, EventTicket>as(persistentKeyValueStore("eventTickets"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_TICKET_JSON)
                );


        // Join the Event Tickets with the Streams on the customer Id, filter where the stream and the event have the same artist Id
        builder
                .stream("data-demo-streams", Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Received: {}", stream))
                .selectKey((streamId, streamRequest) -> streamRequest.customerid() + "-" + streamRequest.artistid())
                .join(
                        eventTicketByCustomerIdTable,
                        (customerArtistId, stream, eventTicket) -> new CustomerStreamEventTicket(stream, eventTicket)
                )
                .peek((customerStreamEventTicketId, customerStreamEventTicket) -> log.info("Stream Received and Joined for Event Ticket: {}", customerStreamEventTicket))
                .filter(((s, customerStreamEventTicket) -> Objects.equals(customerStreamEventTicket.eventTicket.event.artistid(), customerStreamEventTicket.stream.artistid())))
                .peek((customerStreamEventTicketId, customerStreamEventTicket) -> log.info("Filtered Artist Streamed Ticket Event: {}", customerStreamEventTicketId))
                .groupBy(
                        (customerId, customerStream) -> customerStream.stream.artistid(),
                        Grouped.with(Serdes.String(), SERDE_CUSTOMER_STREAM_EVENT_TICKET_JSON)
                )
                .aggregate(
                        SortedCounterMap::new,

                        // aggregator
                        (artistId, customerStream, customerArtistStreamCounts) -> {
                            customerArtistStreamCounts.incrementCount(
                                    customerStream.stream.customerid(),       // customer becomes the map key
                                    customerStream.stream.artistid(),
                                    artistId
                            );
                            return customerArtistStreamCounts;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, SortedCounterMap>as(persistentKeyValueStore("ticketed-customer-artist-stream-counts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(COUNTER_MAP_JSON_SERDE)
                )
                .toStream()
                .peek((sortedCounterMapId, sortedCounterMap) -> log.info("Sorted Counted Map: {}", sortedCounterMap))
                .mapValues(sortedCounterMap -> sortedCounterMap.topWithArtist(bottomNumberStreamers), Named.as("map-to-lowest-streaming-customers-per-artist"))
                .peek((key, value) ->
                        log.info("Lowest Streaming Customers: {}", value)
                )
                .to(LOWEST_STREAMED_TICKETED_CUSTOMERS_TOPIC, Produced.with(Serdes.String(), SERDE_ARTIST_LOWEST_STREAMERS_JSON));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerStreamEventTicket {
        private Stream stream;
        private EventTicket eventTicket;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerStream {
        private String customerId;
        private long streamAmount;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LowestStreamingCustomersPerArtist {
        private String artistId;
        private List<CustomerStream> customersStreamsMap;
    }
    @Data
    @AllArgsConstructor
    public static class SortedCounterMap {
        private int maxSize;
        private String artistId;
        private LinkedHashMap<String, Long> map;

        public SortedCounterMap() {
            this(1000);
        }

        public SortedCounterMap(int maxSize) {
            this.maxSize = maxSize;
            this.map = new LinkedHashMap<>();
        }

        /**
         * Increments the stream count for a given customerId, but ONLY
         * if the stream's artistId matches the target artistId for the event.
         *
         * @param customerId     the customer to track
         * @param streamArtistId the artist on the incoming stream record
         * @param eventArtistId  the artist tied to the event on the ticket
         */
        public void incrementCount(String customerId, String streamArtistId, String eventArtistId) {
            if (!streamArtistId.equals(eventArtistId)) {
                return;
            }

            // Set the artistId the first time we see it
            if (this.artistId == null) {
                this.artistId = eventArtistId;
            }

            map.compute(customerId, (k, v) -> v == null ? 1 : v + 1);

            this.map = map.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .limit(maxSize)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        public List<CustomerStream> top(int limit) {
            return map.entrySet().stream()
                    .limit(limit)
                    .map(e -> new CustomerStream(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
        }

        /**
         * Returns the artistId paired with the top {limit} lowest-streaming customers
         *
         * @param limit the number of customer records to include
         * @return a LowestStreamingCustomersPerArtist containing the artistId and customer stream counts
         */
        public LowestStreamingCustomersPerArtist topWithArtist(int limit) {
            return new LowestStreamingCustomersPerArtist(
                    artistId != null ? artistId : "unknown",
                    top(limit)
            );
        }
    }
}