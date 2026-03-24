package org.improving.workshop.phase3;

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

import static org.improving.workshop.phase3.LeastStreamingTicketHolders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LeastStreamingTicketHoldersTest {
    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, Stream> streamsInputTopic;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Ticket> ticketInputTopic;

    // outputs
    private TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic;

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
                LINKED_HASH_MAP_JSON_SERDE.deserializer()
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
    }


}
