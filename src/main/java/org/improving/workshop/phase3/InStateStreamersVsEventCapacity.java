package org.improving.workshop.phase3;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.io.Serializable;
import java.util.Map;

import static org.improving.workshop.Streams.*;

import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.venue.*;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.customer.address.*;
import org.springframework.kafka.support.serializer.JsonSerde;

public class InStateStreamersVsEventCapacity {

    public static final String OUTPUT_TOPIC = "kafka-workshop-instate-vs-capacity";

    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        
        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }


    static void configureTopology(final StreamsBuilder builder){
        // Create two Ktables for addresses based on if a customerId exists
        // 1. Read input topic as KStream
        KStream<String, Address> addressStream = builder.stream("data-demo-addresses", Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON));
        // 2. Split the stream based on a predicate
        Map<String, KStream<String, Address>> branches = addressStream.split(Named.as("branch-"))
        .branch((key, address) -> address.customerid() == null, Branched.as("non-customer-address"))
        .defaultBranch(Branched.as("customer-address"));
        // 3. Convert branches to KTables
        KTable<String, Address> nonCustomerAddressTable = branches.get("branch-non-customer-address")
        .toTable(Materialized.with(Serdes.String(), SERDE_ADDRESS_JSON)); // Convert to KTable
        KTable<String, Address> customerAddressTable = branches.get("branch-customer-address")
        .map((key, address) -> new KeyValue<>(address.customerid(), address))
        .toTable(Materialized.with(Serdes.String(), SERDE_ADDRESS_JSON)); // Convert to KTable

        //KTable for Venues
        KTable<String, Venue> venueTable = builder.table("data-demo-venues", Consumed.with(Serdes.String(), SERDE_VENUE_JSON));

        // Aggregate Stream Counts by (ArtistID + State)
        KTable<String, Long> streamCountsByCustomerState = builder.stream("data-demo-streams", Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                // Select the new key as customerId
                .selectKey((k,v) -> v.customerid())
                // Join stream to address to find streamer state
                .join(customerAddressTable, 
                        (stream, addr) -> new StreamWithState(stream, addr.state()))
                // Key by "ArtistID:State" for matching to EventVenueAddress later
                .selectKey((k, v) -> v.stream().artistid() + ":" + v.state())
                //group by the key to prep for the count
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(StreamWithState.class)))
                // count all of the artistId:state combinations
                .count(Materialized.as("stream-counts-store"));

        // Trigger Logic: Process Events
        builder.stream("data-demo-events", Consumed.with(Serdes.String(), SERDE_EVENT_JSON))
                //rekey the event to venueid for join
                .selectKey((k,v) -> v.venueid())
                //join the events with venue
                .join(venueTable, 
                    (e, v) -> new EventWithVenue(e, v))
                //rekey to addressId as to join on the address table
                .selectKey((k,v) -> v.venue.addressid())
                //join this EventWithVenue with the noncustomeraddress table
                .join(nonCustomerAddressTable,
                    (eventWithVenue, ncat) -> new EventWithState(eventWithVenue.event(), ncat.state()),
                    Joined.with(Serdes.String(), new JsonSerde<>(EventWithVenue.class), SERDE_ADDRESS_JSON)
                )
                //change the new key to artistid:state to match the streamCountsByCustomerState key above
                .selectKey((k, v) -> v.event.artistid() + ":" + v.state)
                //join streamCountsByCustomerState to EventWithState, creating a new final POJO with all relevant data to business case
                .leftJoin(streamCountsByCustomerState,
                    (l, r) -> new ArtistInStateStreamsAndEventCapacity(l.event.artistid(), r == null ? 0 : r.longValue(), l.state, l.event.capacity()),
                    Joined.with(Serdes.String(), new JsonSerde<>(EventWithState.class), Serdes.Long())
                 )
                 //group by key and reduce to combine all potential in-state events for a given artist with the sum of their capacity
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ArtistInStateStreamsAndEventCapacity.class)))
                .reduce((first, second) 
                    -> new ArtistInStateStreamsAndEventCapacity(first.artistId(), first.streamCount(), first.state(), first.capacity() + second.capacity()))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(ArtistInStateStreamsAndEventCapacity.class)));
    }

        //POJOs for join states
        public record EventWithVenue(Event event, Venue venue) implements Serializable {}
        public record EventWithState(Event event, String state) implements Serializable {}
        public record StreamWithState(Stream stream, String state) implements Serializable {}
        public record ArtistInStateStreamsAndEventCapacity(String artistId, Long streamCount, String state, Integer capacity)  implements Serializable {}
}
