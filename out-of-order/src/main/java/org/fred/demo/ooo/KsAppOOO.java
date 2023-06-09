package org.fred.demo.ooo;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KsAppOOO {

    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String ACKS_TOPIC = "acks";
    public static final String OUTPUT_TOPIC = "output";

    private static final String APPLICATION_ID = "out-of-order";
    private static final String APPLICATION_NAME = "out-of-order";
    static final String TRANSACTION_STATE_STATESTORE = "TransactionsStates";

    private static final Logger LOGGER = LogManager.getLogger();

    public static void main(String[] args) {
        Properties config = getConfig();
        Topology topology = getTopology();

        LOGGER.info("Topology describe: {}", topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            KafkaStreams streams = new KafkaStreams(topology, config);
            setupShutdownHook(streams, latch);
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void setupShutdownHook(KafkaStreams streams, CountDownLatch latch) {
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.err.printf("### Stopping %s Application ###%n", APPLICATION_NAME);
                    streams.close();
                    latch.countDown();
                }));
    }

    static Properties getConfig() {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:10092");

        // Disabling caching ensures we get a complete "changelog" from the
        // aggregate(...) (i.e. every input event will have a corresponding output event.
        // see
        // https://kafka.apache.org/34/documentation/streams/developer-guide/memory-mgmt.html#record-caches-in-the-dsl
        settings.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return settings;
    }

    static Map<String, String> getSerdeConfig() {
        return Collections.singletonMap("schema.registry.url", "http://localhost:8081");
    }

    static Topology getTopology() {
        var serdeConfig = getSerdeConfig();
        final Serde<Transaction> transactionSerde = new SpecificAvroSerde<>();
        transactionSerde.configure(serdeConfig, false);
        final Serde<Ack> ackSerde = new SpecificAvroSerde<>();
        ackSerde.configure(serdeConfig, false);
        final Serde<TransactionState> transactionStateSerde = new SpecificAvroSerde<>();
        transactionStateSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        // Create TransactionState StateStore
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(TRANSACTION_STATE_STATESTORE),
                        Serdes.Long(),
                        transactionStateSerde
                )
        );

        // 1 topic for Transaction
        builder.stream(TRANSACTIONS_TOPIC, Consumed.with(Serdes.Long(), transactionSerde))
                .process(TransactionProcessor::new, Named.as("TransactionProcessor"), TRANSACTION_STATE_STATESTORE)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), transactionStateSerde));


        // 1 topic for Acks
        builder.stream(ACKS_TOPIC, Consumed.with(Serdes.Long(), ackSerde))
                .process(AckProcessor::new, Named.as("AckProcessor"), TRANSACTION_STATE_STATESTORE)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), transactionStateSerde));

        return builder.build();
    }
}
