package org.improving.workshop.phase3;


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
        //2. Venue capacity != event capacity, M in state streams and Y capacity where venue is Z capacity
        //3. Event with 0 total streams (should still report 0 since event was created)
        //4. Event with 0 in-state streams, but some out of state (again, should report 0)
        //5. Artist with multiple events in different states with varying stream count and capacity
        //6. Multiple artists with events in the same state, should yield different outputs
        //7. Streams AFTER event is generated, should be ignored
        //8. Artist with multiple events in the SAME state
        //9. Update customer address to new state, new streams shouldn't be counted toward past state


    //Generate list of venues and addresses
    String venueId_MN = "venue-1";
    String venueId1_Address = "venue-address-1";
    addressInputTopic.pipeInput(venueId1_Address, new Address(venueId1_Address, null, "US", "venue", "111", "222", "ABC", "MN", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId_MN, new Venue(venueId_MN, venueId1_Address, "First Ave", 2000));
    String venueId_WV = "venue-2";
    String venueId2_Address = "venue-address-2";
    addressInputTopic.pipeInput(venueId2_Address, new Address(venueId2_Address, null, "US", "venue", "111", "222", "ABC", "WV", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId_WV, new Venue(venueId_WV, venueId2_Address, "Billy Bobs House", 80));
    String venueId_NY = "venue-3";
    String venueId3_Address = "venue-address-3";
    addressInputTopic.pipeInput(venueId3_Address, new Address(venueId3_Address, null, "US", "venue", "111", "222", "ABC", "NY", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId_NY, new Venue(venueId_NY, venueId3_Address, "Madison Square Garden", 50000));
    String venueId_WI = "venue-4";
    String venueId4_Address = "venue-address-4";
    addressInputTopic.pipeInput(venueId4_Address, new Address(venueId4_Address, null, "US", "venue", "111", "222", "ABC", "WI", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId_WI, new Venue(venueId_WI, venueId4_Address, "Fiserv Forum", 30000));
    String venueId_MN_2 = "venue-5";
    String venueId5_Address = "venue-address-5";
    addressInputTopic.pipeInput(venueId5_Address, new Address(venueId5_Address, null, "US", "venue", "111", "222", "ABC", "MN", "55102", "111", "US", 45.0, 100.0));
    venueInputTopic.pipeInput(venueId_MN_2, new Venue(venueId_MN_2, venueId5_Address, "Myth", 4000));


    //Generate List of Artists for below logic, not fed into topics
    String artistId1 = "123456";
    String artistId2 = "234561";
    String artistId3 = "345612";
    String artistId4 = "456123";


    //Generate streams and addresses associated with customer IDs
    String customerId_MN = "customer-1";
    String customerId1_Address = "customer-address-1";
    addressInputTopic.pipeInput(customerId1_Address, new Address(customerId1_Address, customerId_MN, "US", "venue", "111", "222", "ABC", "MN", "55102", "111", "US", 45.0, 100.0));
    //customer 1 streams
    GenerateStreamEvent(customerId_MN, artistId1, 5, "11111111"); //listen to artist1 5 times
    GenerateStreamEvent(customerId_MN, artistId2, 2, "11111112"); //listen to artist1 2 times
    GenerateStreamEvent(customerId_MN, artistId3, 3, "11111113"); //listen to artist1 3 times


    //Generate streams and addresses associated with customer IDs
    String customerId_AK = "customer-2";
    String customerId2_Address = "customer-address-2";
    addressInputTopic.pipeInput(customerId2_Address, new Address(customerId2_Address, customerId_AK, "US", "venue", "111", "222", "ABC", "AK", "55102", "111", "US", 45.0, 100.0));
    //customer 2 streams
    GenerateStreamEvent(customerId_AK, artistId2, 5, "11111114");
    GenerateStreamEvent(customerId_AK, artistId1, 2, "11111115");
    GenerateStreamEvent(customerId_AK, artistId3, 3, "11111116");


    //Generate streams and addresses associated with customer IDs
    String customerId_WV = "customer-3";
    String customerId3_Address = "customer-address-3";
    addressInputTopic.pipeInput(customerId3_Address, new Address(customerId3_Address, customerId_WV, "US", "venue", "111", "222", "ABC", "WV", "55102", "111", "US", 45.0, 100.0));
    //customer 3 streams
    GenerateStreamEvent(customerId_WV, artistId1, 4, "11111117");
    GenerateStreamEvent(customerId_WV, artistId3, 2, "11111118");
    GenerateStreamEvent(customerId_WV, artistId2, 3, "11111119");
    // update customer3's address for Test case 9
    String customerId3_Address2 = "customer-address-4";
    addressInputTopic.pipeInput(customerId3_Address2, new Address(customerId3_Address2, customerId_WV, "US", "venue", "111", "222", "ABC", "KS", "55102", "111", "US", 45.0, 100.0));
    // listen to artist2 more, as they have an event in their previous address' state
    GenerateStreamEvent(customerId_WV, artistId2, 2, "11111120");


    //Generate events tied to venues above
    String eventId_MN = "event-1";
    eventInputTopic.pipeInput(eventId_MN, new Event(eventId_MN, artistId1, venueId_MN, 2000, "01012026"));
    String eventId_WV = "event-2";
    eventInputTopic.pipeInput(eventId_WV, new Event(eventId_WV, artistId2, venueId_WV, 50, "01312026"));
    String eventId_NY = "event-3";
    eventInputTopic.pipeInput(eventId_NY, new Event(eventId_NY, artistId4, venueId_NY, 50000, "02102026"));
    String eventId_WI = "event-4";
    eventInputTopic.pipeInput(eventId_WI, new Event(eventId_WI, artistId1, venueId_WI, 30000, "02202026"));
    String eventId_WV_2 = "event-5";
    eventInputTopic.pipeInput(eventId_WV_2, new Event(eventId_WV_2, artistId1, venueId_WV, 50, "02282026"));


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
    GenerateStreamEvent(customerId_WV, artistId1, 5, "11111111"); //listen to artist1 5 times
    outputRecords.addAll(outputTopic.readRecordsToList()); //repoll the output topic
    assertEquals(5, outputRecords.size()); //the size should not have changed, should still be 5 which is the same as number of events

    //Test case 8 - Artist with multiple events in the SAME state
    //
    //add a second MN event for artist1
    String eventId_MN_2 = "event-6";
    eventInputTopic.pipeInput(eventId_MN_2, new Event(eventId_MN_2, artistId1, venueId_MN_2, 4000, "03012026"));
    outputRecords.addAll(outputTopic.readRecordsToList()); //repoll the output topic
    assertEquals(6, outputRecords.size()); //the size should have increased by one with the new event
    assertEquals(5, outputRecords.get(5).value().streamCount()); //two events for one artist's in-state stream count should be 5
    assertEquals(6000, outputRecords.get(5).value().capacity()); //event's stream capacity should now be 6,000 (event1 2,000 + event6 4,000)
    assertEquals(artistId1, outputRecords.get(5).value().artistId()); //event's artist should be artist1
    assertEquals("MN", outputRecords.get(5).value().state()); //event's state should be MN


    return;
  }
}



