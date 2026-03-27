package org.improving.workshop.phase3;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.improving.workshop.phase3.LeastStreamingTicketHolders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class LeastStreamingTicketHoldersTest {
    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, Stream> streamsInputTopic;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Ticket> ticketInputTopic;

    // outputs
    private TestOutputTopic<String, LinkedHashMap<String, LinkedHashMap<String, Long>>> outputTopic;

    @BeforeEach
    void setup() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the CustomerStreamCount topology (by reference)
        leastStreamingTicketHoldersTopology(streamsBuilder, 2);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        streamsInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                stringSerializer,
                Streams.SERDE_STREAM_JSON.serializer()
        );

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                stringSerializer,
                Streams.SERDE_EVENT_JSON.serializer()
        );

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                stringSerializer,
                Streams.SERDE_TICKET_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
                LOWEST_STREAMED_TICKETED_CUSTOMERS_TOPIC,
                stringDeserializer,
                SERDE_ARTIST_LOWEST_STREAMERS_JSON.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close();
    }

    /**
     * Scenario: Event has 3 customers, each which have different number of streams for the event's artist.
     * Only the 2, lowest streaming customers should be shown
     */
    @Test
    @DisplayName("lowest streaming customers of an artist for an event they have a ticket for")
    void lowestStreamingTicketedCustomer() {
        // Event
        String eventId = "event-123";
        String artist = "TaylorSwift";
        eventInputTopic.pipeInput(eventId, new Event(eventId, artist, "venue-123", 5, "tomorrow"));

        // Tickets for an event
        String customerId1 = "customer-1";
        String customerId2 = "customer-2";
        String customerId3 = "customer-3";

        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId1, eventId));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId2, eventId));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId3, eventId));

        // Streams for the Artist (customer 2 and 3 only have 1 stream)
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId3, artist));

        var outputRecords = outputTopic.readRecordsToList();

        assertEquals(4, outputRecords.size());
        //the last record holds the initial top 3 state
        LinkedHashMap<String, LinkedHashMap<String, Long>> lastResult = outputRecords.getLast().getValue();
        // assert the artist key
        assertTrue(lastResult.containsKey(artist));

        // assert customer1 and customer2 are the lowest streamers
        LinkedHashMap<String, Long> lowestStreamers = lastResult.get(artist);
        assertEquals(2, lowestStreamers.size());
        assertTrue(lowestStreamers.containsKey(customerId2));
        assertTrue(lowestStreamers.containsKey(customerId3));
    }

    /**
     * Scenario: Event has 3 customers, each which have different number of streams for the event's artist.
     * There are 2 extra customers that don't have tickets but stream the artist
     * Only the 2, lowest streaming customers should be shown
     */
    @Test
    @DisplayName("lowest streaming customers of an artist for an event they have a ticket for")
    void lowestStreamingTicketedCustomerWithExtraCustomers() {
        // Event
        String eventId = "event-123";
        String artist = "TaylorSwift";
        eventInputTopic.pipeInput(eventId, new Event(eventId, artist, "venue-123", 5, "tomorrow"));

        // Tickets for an event
        String customerId1 = "customer-1";
        String customerId2 = "customer-2";
        String customerId3 = "customer-3";
        String customerId4 = "customer-4";
        String customerId5 = "customer-5";

        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId1, eventId));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId2, eventId));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId3, eventId));

        // Streams for the Artist (customer 2 and 3 only have 1 stream)
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId4, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId5, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId3, artist));

        var outputRecords = outputTopic.readRecordsToList();

        assertEquals(4, outputRecords.size());
        //the last record holds the initial top 3 state
        LinkedHashMap<String, LinkedHashMap<String, Long>> lastResult = outputRecords.getLast().getValue();
        // assert the artist key
        assertTrue(lastResult.containsKey(artist));

        // assert customer1 and customer2 are the lowest streamers since they have a ticket
        LinkedHashMap<String, Long> lowestStreamers = lastResult.get(artist);
        assertEquals(2, lowestStreamers.size());
        assertTrue(lowestStreamers.containsKey(customerId2));
        assertTrue(lowestStreamers.containsKey(customerId3));
    }

    /**
     * Scenario: Validate as streams come in that the lowest streamed updates
     * There are 2 extra customers that don't have tickets but stream the artist
     * Only the 2, lowest streaming customers should be shown
     */
    @Test
    @DisplayName("lowest streaming customers of an artist for an event they have a ticket for")
    void lowestStreamingTicketedCustomerContinuous() {
        // Event
        String eventId = "event-123";
        String artist = "TaylorSwift";
        eventInputTopic.pipeInput(eventId, new Event(eventId, artist, "venue-123", 5, "tomorrow"));

        // Tickets for an event
        String customerId1 = "customer-1";
        String customerId2 = "customer-2";
        String customerId3 = "customer-3";
        String customerId4 = "customer-4";
        String customerId5 = "customer-5";

        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId1, eventId));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId2, eventId));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId3, eventId));

        // Streams for the Artist (customer 2 and 3 only have 1 stream)
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId4, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId5, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId3, artist));

        var outputRecords = outputTopic.readRecordsToList();

        assertEquals(4, outputRecords.size());
        //the last record holds the initial top 3 state
        LinkedHashMap<String, LinkedHashMap<String, Long>> lastResult = outputRecords.getLast().getValue();
        // assert the artist key
        assertTrue(lastResult.containsKey(artist));

        // assert customer1 and customer2 are the lowest streamers since they have a ticket
        LinkedHashMap<String, Long> lowestStreamers = lastResult.get(artist);
        assertEquals(2, lowestStreamers.size());
        assertTrue(lowestStreamers.containsKey(customerId2));
        assertTrue(lowestStreamers.containsKey(customerId3));

        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId3, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId3, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));

        var outputRecords2 = outputTopic.readRecordsToList();
        LinkedHashMap<String, LinkedHashMap<String, Long>> lastResult2 = outputRecords2.getLast().getValue();
        LinkedHashMap<String, Long> lowestStreamers2 = lastResult2.get(artist);
        assertEquals(2, lowestStreamers2.size());
        assertTrue(lowestStreamers2.containsKey(customerId3));
        assertTrue(lowestStreamers2.containsKey(customerId1));
    }
    /**
     * Scenario: 2 Event has 3 customers, each which have different number of streams for the event's artist.
     * Only the 2, lowest streaming customers should be shown for each Event
     */
    @Test
    @DisplayName("lowest streaming customers of an artist for an event they have a ticket for")
    void lowestStreamingTicketedCustomerForMultipleEvents() {
        // Event
        String eventId = "event-123";
        String eventId2 = "event-987";
        String artist = "TaylorSwift";
        String artist2 = "LorienTestard";
        eventInputTopic.pipeInput(eventId, new Event(eventId, artist, "venue-123", 5, "tomorrow"));
        eventInputTopic.pipeInput(eventId2, new Event(eventId2, artist2, "venue-123", 5, "tomorrow"));

        // Tickets for an event
        String customerId1 = "customer-1";
        String customerId2 = "customer-2";
        String customerId3 = "customer-3";
        String customerId4 = "customer-4";

        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId1, eventId));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId2, eventId));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId3, eventId));

        // Streams for the Artist (customer 2 and 3 only have 1 stream)
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId3, artist));

        var outputRecords = outputTopic.readRecordsToList();

        assertEquals(4, outputRecords.size());
        //the last record holds the initial top 3 state
        LinkedHashMap<String, LinkedHashMap<String, Long>> lastResult = outputRecords.getLast().getValue();
        // assert the artist key
        assertTrue(lastResult.containsKey(artist));

        // assert customer1 and customer2 are the lowest streamers since they have a ticket
        LinkedHashMap<String, Long> lowestStreamers = lastResult.get(artist);
        assertEquals(2, lowestStreamers.size());
        assertTrue(lowestStreamers.containsKey(customerId2));
        assertTrue(lowestStreamers.containsKey(customerId3));

        // tickets for event 2 with different artist
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId1, eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId2, eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId4, eventId2));

        // Streams for Arist 2 (customer 1 and 4 have 1 stream
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId1, artist2));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist2));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist2));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId3, artist2));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist2));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId4, artist2));

        var outputRecords2 = outputTopic.readRecordsToList();

        lastResult = outputRecords2.getLast().getValue();
        assertTrue(lastResult.containsKey(artist2));
        lowestStreamers = lastResult.get(artist2);
        assertEquals(2, lowestStreamers.size());
        assertTrue(lowestStreamers.containsKey(customerId1));
        assertTrue(lowestStreamers.containsKey(customerId4));

        // Add another stream for artist1
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId2, artist));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customerId4, eventId));
        streamsInputTopic.pipeInput(DataFaker.STREAMS.generate(customerId4, artist));

        var outputRecords3 = outputTopic.readRecordsToList();

        log.info(outputRecords3.toString());
        lastResult = outputRecords3.getLast().getValue();
        assertTrue(lastResult.containsKey(artist));
        lowestStreamers = lastResult.get(artist);
        assertEquals(2, lowestStreamers.size());
        assertTrue(lowestStreamers.containsKey(customerId3));
        assertTrue(lowestStreamers.containsKey(customerId4));

    }


}
