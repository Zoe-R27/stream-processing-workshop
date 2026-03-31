package org.improving.workshop.exercises.phase3;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;


import static org.junit.jupiter.api.Assertions.assertEquals;




public class InStateStreamTest {
  private TopologyTestDriver driver;


  private TestInputTopic<String, Event> eventInputTopic;
  private TestInputTopic<String, Stream> streamInputTopic;
  private TestInputTopic<String, Venue> venueInputTopic;
  private TestInputTopic<String, Address> addressInputTopic;


  private TestOutputTopic<String, InStateStreamersVsEventCapacity.ArtistInStateStreamsAndEventCapacity> outputTopic;




  public static final JsonSerde<InStateStreamersVsEventCapacity.ArtistInStateStreamsAndEventCapacity> SERDE_AISSAEC_JSON = new JsonSerde<>(InStateStreamersVsEventCapacity.ArtistInStateStreamsAndEventCapacity.class);


  @BeforeEach
  public void setup() {


    StreamsBuilder streamsBuilder = new StreamsBuilder();
    InStateStreamersVsEventCapacity.configureTopology(streamsBuilder);


    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());


    eventInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_EVENTS,
            Serdes.String().serializer(),
            Streams.SERDE_EVENT_JSON.serializer()
    );


    streamInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_STREAMS,
            Serdes.String().serializer(),
            Streams.SERDE_STREAM_JSON.serializer()
    );


    venueInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_VENUES,
            Serdes.String().serializer(),
            Streams.SERDE_VENUE_JSON.serializer()
    );


    addressInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_ADDRESSES,
            Serdes.String().serializer(),
            Streams.SERDE_ADDRESS_JSON.serializer()
    );


    outputTopic = driver.createOutputTopic(
            InStateStreamersVsEventCapacity.OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            SERDE_AISSAEC_JSON.deserializer()
    );
  }


  @AfterEach
  public void cleanup() {
    driver.close();
  }


  public String RandomIDGenerator() {
        int min = 100000;
        int max = 999999;
        int randomNum = min + (int)(Math.random() * ((max - min) + 1));
        return Integer.toString(randomNum);
  }


  public void GenerateStreamEvent(String customerId, String artistId, int numListens, String streamtime) {
        for(int i = 0; i < numListens; i++)
        {
            String streamId = RandomIDGenerator();
            streamInputTopic.pipeInput(streamId, new Stream(streamId, customerId, artistId, streamtime));
        }
  }


  @Test
  @DisplayName("in state streams vs event capacity")
  public void testInstate() {
    //Test Cases covered in this:
        //1. Typical case, Event with N in state Streams and X capacity
        //TODO am I ever counting more than one in-state customer streaming an artist?
        //2. Venue capacity != event capacity, M in state streams and Y capacity where venue is Z capacity
        //3. Event with 0 total streams (should still report 0 since event was created)
        //4. Event with 0 in-state streams, but some out of state (again, should report 0)
        //5. Artist with multiple events in different states with varying stream count and capacity
        //6. Multiple artists with events in the same state, should yield different outputs
        //7. Streams AFTER event is generated, should be ignored
        //8. Artist with multiple events in the SAME state
        //9. Update customer address to new state, new streams shouldn't be counted toward past state


    //Generate list of venues and addresses
    String venueId1 = "venue-1";
    String venueId1_Address = "venue-address-1";
    addressInputTopic.pipeInput(venueId1_Address, new Address(venueId1_Address, null, "US", "venue", "111", "222", "ABC", "MN", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId1, new Venue(venueId1, venueId1_Address, "First Ave", 2000));
    String venueId2 = "venue-2";
    String venueId2_Address = "venue-address-2";
    addressInputTopic.pipeInput(venueId2_Address, new Address(venueId2_Address, null, "US", "venue", "111", "222", "ABC", "WV", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId2, new Venue(venueId2, venueId2_Address, "Billy Bobs House", 80));
    String venueId3 = "venue-3";
    String venueId3_Address = "venue-address-3";
    addressInputTopic.pipeInput(venueId3_Address, new Address(venueId3_Address, null, "US", "venue", "111", "222", "ABC", "NY", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId3, new Venue(venueId3, venueId3_Address, "Madison Square Garden", 50000));
    String venueId4 = "venue-4";
    String venueId4_Address = "venue-address-4";
    addressInputTopic.pipeInput(venueId4_Address, new Address(venueId4_Address, null, "US", "venue", "111", "222", "ABC", "WI", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId4, new Venue(venueId4, venueId4_Address, "Fiserv Forum", 30000));
    String venueId5 = "venue-5";
    String venueId5_Address = "venue-address-5";
    addressInputTopic.pipeInput(venueId5_Address, new Address(venueId5_Address, null, "US", "venue", "111", "222", "ABC", "MN", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId5, new Venue(venueId5, venueId5_Address, "Myth", 4000));


    //Generate List of Artists for below logic, not fed into topics
    String artistId1 = "123456";
    String artistId2 = "234561";
    String artistId3 = "345612";
    String artistId4 = "456123";


    //Generate streams and addresses associated with customer IDs
    String customerId1 = "customer-1";
    String customerId1_Address = "customer-address-1";
    addressInputTopic.pipeInput(customerId1_Address, new Address(customerId1_Address, customerId1, "US", "venue", "111", "222", "ABC", "MN", "55102", "111", "US", 45.0, 100.0));
    //customer 1 streams
    GenerateStreamEvent(customerId1, artistId1, 5, "11111111"); //listen to artist1 5 times
    GenerateStreamEvent(customerId1, artistId2, 2, "11111112"); //listen to artist1 2 times
    GenerateStreamEvent(customerId1, artistId3, 3, "11111113"); //listen to artist1 3 times


    //Generate streams and addresses associated with customer IDs
    String customerId2 = "customer-2";
    String customerId2_Address = "customer-address-2";
    addressInputTopic.pipeInput(customerId2_Address, new Address(customerId2_Address, customerId2, "US", "venue", "111", "222", "ABC", "AK", "55102", "111", "US", 45.0, 100.0));
    //customer 2 streams
    GenerateStreamEvent(customerId2, artistId2, 5, "11111114");
    GenerateStreamEvent(customerId2, artistId1, 2, "11111115");
    GenerateStreamEvent(customerId2, artistId3, 3, "11111116");


    //Generate streams and addresses associated with customer IDs
    String customerId3 = "customer-3";
    String customerId3_Address = "customer-address-3";
    addressInputTopic.pipeInput(customerId3_Address, new Address(customerId3_Address, customerId3, "US", "venue", "111", "222", "ABC", "WV", "55102", "111", "US", 45.0, 100.0));
    //customer 3 streams
    GenerateStreamEvent(customerId3, artistId1, 4, "11111117");
    GenerateStreamEvent(customerId3, artistId3, 2, "11111118");
    GenerateStreamEvent(customerId3, artistId2, 3, "11111119");
    // update customer3's address for Test case 9
    String customerId3_Address2 = "customer-address-4";
    addressInputTopic.pipeInput(customerId3_Address2, new Address(customerId3_Address2, customerId3, "US", "venue", "111", "222", "ABC", "KS", "55102", "111", "US", 45.0, 100.0));
    // listen to artist2 more, as they have an event in their previous address' state
    GenerateStreamEvent(customerId3, artistId2, 2, "11111120");


    //Generate events tied to venues above
    String eventId1 = "event-1";
    eventInputTopic.pipeInput(eventId1, new Event(eventId1, artistId1, venueId1, 2000, "01012026"));
    String eventId2 = "event-2";
    eventInputTopic.pipeInput(eventId2, new Event(eventId2, artistId2, venueId2, 50, "01312026"));
    String eventId3 = "event-3";
    eventInputTopic.pipeInput(eventId3, new Event(eventId3, artistId4, venueId3, 50000, "02102026"));
    String eventId4 = "event-4";
    eventInputTopic.pipeInput(eventId4, new Event(eventId4, artistId1, venueId4, 30000, "02202026"));
    String eventId5 = "event-5";
    eventInputTopic.pipeInput(eventId5, new Event(eventId5, artistId1, venueId2, 50, "02282026"));


    //Check output
    var outputRecords = outputTopic.readRecordsToList();
    assertEquals(5, outputRecords.size()); //should be one record for each event

    //Test case 1 - Typical case, Event with N in state Streams and X capacity
    assertEquals(5, outputRecords.get(0).value().streamCount()); //event's in-state stream count should be 5
    assertEquals(2000, outputRecords.get(0).value().capacity()); //event's stream capacity should be 2000
    assertEquals(artistId1, outputRecords.get(0).value().artistId()); //event's artist should be artist1
    assertEquals("MN", outputRecords.get(0).value().state()); //event's state should be MN

    //     Test case 2 - Venue capacity != event capacity, M in state streams and Y capacity where venue is Z capacity
    //AND  Test case 9 - Update customer address, don't count new streams from that artist but preserve old ones
    assertEquals(3, outputRecords.get(1).value().streamCount()); //event's in-state stream count should be 3
    assertEquals(50, outputRecords.get(1).value().capacity()); //event's stream capacity should be 50 (note, different from venue capacity)
    assertEquals(artistId2, outputRecords.get(1).value().artistId()); //event's artist should be artist2
    assertEquals("WV", outputRecords.get(1).value().state()); //event's state should be WV

    //Test case 3  - Event with 0 total streams (should still report 0 since event was created)
    assertEquals(0, outputRecords.get(2).value().streamCount()); //event's in-state stream count should be 0
    assertEquals(50000, outputRecords.get(2).value().capacity()); //event's stream capacity should be 50,000
    assertEquals(artistId4, outputRecords.get(2).value().artistId()); //event's artist should be artist5
    assertEquals("NY", outputRecords.get(2).value().state()); //event's state should be NY

    //Test case 4  - Event with 0 in-state streams, but some out of state (again, should report 0)
    assertEquals(0, outputRecords.get(3).value().streamCount()); //event's in-state stream count should be 0
    assertEquals(30000, outputRecords.get(3).value().capacity()); //event's stream capacity should be 30,000
    assertEquals(artistId1, outputRecords.get(3).value().artistId()); //event's artist should be artist1
    assertEquals("WI", outputRecords.get(3).value().state()); //event's state should be WI

    //     Test case 5 - Artist with multiple events in different states with varying stream count and capacity
    //And  Test case 6 - Multiple artists with events in the same state, should yield different outputs
    assertEquals(4, outputRecords.get(4).value().streamCount()); //event's in-state stream count should be 4
    assertEquals(50, outputRecords.get(4).value().capacity()); //event's stream capacity should be 50
    assertEquals(artistId1, outputRecords.get(4).value().artistId()); //event's artist should be artist1
    assertEquals("WV", outputRecords.get(4).value().state()); //event's state should be WV
   
    //Test case 7 - Streams AFTER event is generated should be ignored
    GenerateStreamEvent(customerId2, artistId1, 5, "11111111"); //listen to artist1 5 times
    outputRecords.addAll(outputTopic.readRecordsToList()); //repoll the output topic
    assertEquals(5, outputRecords.size()); //the size should not have changed, should still be 5 which is the same as number of events

    //Test case 8 - Artist with multiple events in the SAME state
    //
    //add a second MN event for artist1
    String eventId6 = "event-6";
    eventInputTopic.pipeInput(eventId6, new Event(eventId6, artistId1, venueId5, 4000, "03012026"));
    outputRecords.addAll(outputTopic.readRecordsToList()); //repoll the output topic
    assertEquals(6, outputRecords.size()); //the size should have increased by one with the new event
    assertEquals(5, outputRecords.get(5).value().streamCount()); //two events for one artist's in-state stream count should be 5
    assertEquals(6000, outputRecords.get(5).value().capacity()); //event's stream capacity should now be 6,000 (event1 2,000 + event6 4,000)
    assertEquals(artistId1, outputRecords.get(5).value().artistId()); //event's artist should be artist1
    assertEquals("MN", outputRecords.get(5).value().state()); //event's state should be MN


    return;
  }
}



