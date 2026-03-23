package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.venue.Venue;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.customer.address.Address;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class OutOfStateTicketHoldersTest {

    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    private TopologyTestDriver driver;

    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Ticket> ticketInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestInputTopic<String, Venue> venueInputTopic;
    private TestOutputTopic<String, OutOfStateTicketHolders.CustomerAddressTicket> outputTopic;

    @BeforeEach
    void setup() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the RemainingEventTickets topology (by reference)
        OutOfStateTicketHolders.configureTopology(streamsBuilder);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

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

        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                stringSerializer,
                Streams.SERDE_ADDRESS_JSON.serializer()
        );

        venueInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_VENUES,
                stringSerializer,
                Streams.SERDE_VENUE_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
                PurchaseEventTicket.OUTPUT_TOPIC,
                stringDeserializer,
                OutOfStateTicketHolders.CUSTOMER_ADDRESS_TICKET_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close();
    }

    @Test
    @DisplayName("out of state ticket holder")
    void purchaseOutStateTickets() {
        String venueAddressId = "venue-address-1";
        Address venueAddress = new Address(venueAddressId, null, "cd", "VENUE", "123 Fake St", "Suite 200",
                "Springfield", "MN", "55409", "1234", "USA", 0.0, 0.0);

        addressInputTopic.pipeInput(venueAddressId, venueAddress);

        String venueId = "venue-1";

        venueInputTopic.pipeInput(venueId, new Venue(venueId, venueAddressId, "greate venue", 5));

        String eventId = "exciting-event-123";

        eventInputTopic.pipeInput(eventId, new Event(eventId, "artist-1", venueId, 5, "today"));

        String customer1AddressId = "customer-address-1";
        String customer1Id = "customer-1";
        Address customer1Address = new Address(customer1AddressId, customer1Id, "cd", "HOME", "223 Fake St", "Apt 203",
                "Springfield", "MN", "55409", "1234", "USA", 0.0, 0.0);

        addressInputTopic.pipeInput(customer1AddressId, customer1Address);
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customer1Id, eventId));

        String customer2AddressId = "customer-address-2";
        String customer2Id = "customer-2";
        Address customer2Address = new Address(customer2AddressId, customer2Id, "cd", "HOME", "1234 Lake St", null,
                "Reno", "NV", "85409", "1234", "USA", 0.0, 0.0);

        addressInputTopic.pipeInput(customer2AddressId, customer2Address);
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customer2Id, eventId));

        var outputRecords = outputTopic.readRecordsToList();

        assertEquals(1, outputRecords.size());

        TestRecord<String, OutOfStateTicketHolders.CustomerAddressTicket> customerAddressTicket = outputRecords.getFirst();
        assertEquals(customer2Id, customerAddressTicket.value().getCustomerId());
        assertEquals(customer2Id, customerAddressTicket.value().getCustomerId());
    }

    @Test
    @DisplayName("ignore in state ticket holder")
    void purchaseInStateTicket() {
        String venueAddressId = "venue-address-1";
        Address venueAddress = new Address(venueAddressId, null, "cd", "VENUE", "123 Fake St", "Suite 200",
                "Springfield", "MN", "55409", "1234", "USA", 0.0, 0.0);

        addressInputTopic.pipeInput(venueAddressId, venueAddress);

        String venueId = "venue-1";

        venueInputTopic.pipeInput(venueId, new Venue(venueId, venueAddressId, "greate venue", 5));

        String eventId = "exciting-event-123";

        eventInputTopic.pipeInput(eventId, new Event(eventId, "artist-1", venueId, 5, "today"));

        String customer1AddressId = "customer-address-1";
        String customer1Id = "customer-1";
        Address customer1Address = new Address(customer1AddressId, customer1Id, "cd", "HOME", "223 Fake St", "Apt 203",
                "Springfield", "MN", "55409", "1234", "USA", 0.0, 0.0);

        addressInputTopic.pipeInput(customer1AddressId, customer1Address);
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate(customer1Id, eventId));

        var outputRecords = outputTopic.readRecordsToList();

        assertEquals(0, outputRecords.size());
    }

    @Test
    @DisplayName("only out of state ticket holder")
    void purchaseInAndOutStateTicket() {

        String venue1AddressId = "venue-address-1";
        Address venue1Address = new Address(venue1AddressId, null, "cd", "VENUE", "123 Fake St", "Suite 200",
                "Springfield", "MN", "55409", "1234", "USA", 0.0, 0.0);
        addressInputTopic.pipeInput(venue1AddressId, venue1Address);

        String venue2AddressId = "venue-address-2";
        Address venue2Address = new Address(venue2AddressId, null, "cd", "VENUE", "321 Mock Ave", "Suite 300",
                "Hudson", "WI", "45409", "2234", "USA", 0.0, 0.0);
        addressInputTopic.pipeInput(venue2AddressId, venue2Address);

        String[] eventIds = {
                "MN-event-1",
                "MN-event-2",
                "MN-event-3",
                "WI-event-1"
        };

        for (String eventId: eventIds) {
            if (eventId.contains("MN")) {
                eventInputTopic.pipeInput(eventId, new Event(eventId, "artist-1", venue1AddressId, 5, "today"));
            } else if (eventId.contains("WI")) {
                eventInputTopic.pipeInput(eventId, new Event(eventId, "artist-1", venue2AddressId, 5, "today"));
            }
        }

        ArrayList<MockCustomer> customers = new ArrayList<>(List.of(
                // multiple in state and one out of state
                new MockCustomer("c1", "MN", new ArrayList<>(List.of("MN-event-1", "MN-event-2", "MN-event-3", "WI-event-1"))),
                // instate match only
                new MockCustomer("c2", "MN", new ArrayList<>(List.of("MN-event-2"))),
                // instate match only
                new MockCustomer("c3", "WI", new ArrayList<>(List.of("WI-event-1"))),
                // only out-of-state matches
                new MockCustomer("c4", "FL", new ArrayList<>(List.of("MN-event-1", "MN-event-2", "WI-event-1"))),
                // multiple out-of-state one instate
                new MockCustomer("c5", "WI", new ArrayList<>(List.of("MN-event-1", "MN-event-2", "MN-event-3", "WI-event-1"))),
                // multiple instate, one out-of-state
                new MockCustomer("c6", "MN", new ArrayList<>(List.of("MN-event-1", "MN-event-2", "MN-event-3", "WI-event-1"))),
                // no events
                new MockCustomer("c7", "IA", new ArrayList<>())
        ));

        HashMap<String,ArrayList<String>> expectedEvents = new HashMap<String,ArrayList<String>>();
        int expectedEventCount = 0;
        for (MockCustomer customer: customers) {
            ArrayList<String> expectedCustomerEvents = customer.expectedOutOfStateEvents();
            expectedEventCount += expectedCustomerEvents.size();
            expectedEvents.put(customer.customerId, expectedCustomerEvents);

            addressInputTopic.pipeInput(customer.address.id(), customer.address);
            for (Ticket ticket: customer.tickets) {
                ticketInputTopic.pipeInput(ticket);
            }
        }

        var outputRecords = outputTopic.readRecordsToList();

        assertEquals(expectedEventCount, outputRecords.size());
        for (TestRecord<String, OutOfStateTicketHolders.CustomerAddressTicket> record: outputRecords) {
            var customer = record.key();
            OutOfStateTicketHolders.CustomerAddressTicket cat = record.value();
            assertEquals(customer, cat.getCustomerId());
            var eventId = cat.getTicket().eventid();
            // eventid has state code at the beginning of eventid. Assert customer address state is different
            assert(!eventId.contains(cat.getCustomerAddress().state()));
            assert(expectedEvents.getOrDefault(customer,new ArrayList<>()).contains(eventId));
            // remove matches from set of expected events
            expectedEvents.get(customer).remove(eventId);
        }
        for (String customerId: expectedEvents.keySet()) {
            // assert all expected events were matched in the output and removed
            // and there are no unexpected events
            assertEquals(0, expectedEvents.get(customerId).size());
        }
    }

    public static class MockCustomer {
        private final String customerId;
        private final Address address;
        private final ArrayList<Ticket> tickets;

        public MockCustomer(String customerId, String state, ArrayList<String> eventIds) {
            this.customerId = customerId;
            String addressId = String.format("%s-address", customerId);
            this.address = new Address(addressId, customerId, "cd", "HOME", "123 Fake St", "Apt 5",
            "Springfield", state, "55555", "1234", "USA", 0.0, 0.0);
            this.tickets = new ArrayList<Ticket>();

            for (String eventId: eventIds) {
                this.tickets.add(DataFaker.TICKETS.generate(customerId,eventId));
            }
        }

        public ArrayList<String> expectedOutOfStateEvents() {
            ArrayList<String> expectedEvents = new ArrayList<>();
            if (this.tickets.isEmpty()) {
                return expectedEvents;
            }
            for (Ticket ticket: this.tickets) {
                var eventId = ticket.eventid();
                if (eventId.contains(this.address.state())) {
                    expectedEvents.add(eventId);
                }
            }
            return expectedEvents;
        }
    }
}
