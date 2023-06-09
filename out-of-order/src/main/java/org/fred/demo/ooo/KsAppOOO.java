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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KsAppOOO {

    private static final String APPLICATION_ID = "out-of-order";
    private static final String APPLICATION_NAME = "out-of-order";
    private static final String TRANSACTION_STATE_STATESTORE = "TransactionsStates";

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
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
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
        return Collections.singletonMap(
                "schema.registry.url", "http://localhost:8081");
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
        var transactionStateStore = builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(TRANSACTION_STATE_STATESTORE),
                        Serdes.Long(),
                        transactionStateSerde
                )
        );

        // 1 topic for Transaction
        var transactionStream = builder.stream("transactions", Consumed.with(Serdes.Long(), transactionSerde));

        transactionStream.process(() -> new Processor<Long, Transaction, Long, TransactionState>() {
                    private ProcessorContext<Long, TransactionState> context;
                    private KeyValueStore<Long, TransactionState> transactionStateStateStore;

                    @Override
                    public void init(ProcessorContext<Long, TransactionState> context) {
                        Processor.super.init(context);
                        this.context = context;
                        this.transactionStateStateStore = context.getStateStore(TRANSACTION_STATE_STATESTORE);
                        context.schedule(Duration.ofSeconds(20), PunctuationType.WALL_CLOCK_TIME, ts -> {
                            LOGGER.debug("Punctuate for expired transaction");
                            try (var it = transactionStateStateStore.all()) {
                                while (it.hasNext()) {
                                    var keyValue = it.next();
                                    var transactionTs = keyValue.value.getTransactionTs();
                                    if (transactionTs != null) {
                                        if (ts - transactionTs > 100_000L) {
                                            LOGGER.info("Transaction expired id={}", keyValue.key);
                                            var transactionState = new TransactionState();
                                            transactionState.setId(keyValue.key);
                                            transactionState.setTransactionTs(keyValue.value.getTransactionTs());
                                            transactionState.setData(keyValue.value.getData());
                                            transactionState.setAckTs(0L);
                                            transactionState.setAckData("EXPIRED");
                                            context.forward(new Record<>(keyValue.key, transactionState, ts));
                                            transactionStateStateStore.delete(keyValue.key);
                                        }
                                    }
                                }
                            }
                        });
                    }

                    @Override
                    public void process(Record<Long, Transaction> record) {
                        LOGGER.debug("TransactionProcessor processing record {}", record);
                        var prevRecord = this.transactionStateStateStore.get(record.key());
                        if (prevRecord == null) {
                            var newRecord = new TransactionState();
                            newRecord.setId(record.key());
                            newRecord.setTransactionTs(record.value().getTs());
                            newRecord.setData(record.value().getData());
                            this.transactionStateStateStore.put(record.key(), newRecord);
                            this.context.commit();
                            LOGGER.info("New Transaction id={}", record.key());
                        } else {
                            if (prevRecord.getAckTs() == null) {
                                LOGGER.warn("duplicate Transaction id={}", record.key());
                            } else {
                                var newRecord = new TransactionState();
                                newRecord.setId(record.key());
                                newRecord.setTransactionTs(record.value().getTs());
                                newRecord.setData(record.value().getData());
                                newRecord.setAckTs(prevRecord.getAckTs());
                                newRecord.setAckData(prevRecord.getAckData());
                                this.context.forward(new Record<>(newRecord.getId(), newRecord, record.timestamp()));
                                this.transactionStateStateStore.delete(record.key());
                                this.context.commit();
                                LOGGER.info("Ack found for transaction id={}", record.key());
                            }
                        }
                    }
                },
                TRANSACTION_STATE_STATESTORE
        ).to("output", Produced.with(Serdes.Long(), transactionStateSerde));

        // 1 topic for Acks
        var ackStream = builder.stream("acks", Consumed.with(Serdes.Long(), ackSerde));

        ackStream.process(() -> new Processor<Long, Ack, Long, TransactionState>() {
                    private ProcessorContext<Long, TransactionState> context;
                    private KeyValueStore<Long, TransactionState> transactionStateStateStore;

                    @Override
                    public void init(ProcessorContext<Long, TransactionState> context) {
                        Processor.super.init(context);
                        this.context = context;
                        this.transactionStateStateStore = context.getStateStore(TRANSACTION_STATE_STATESTORE);
                    }

                    @Override
                    public void process(Record<Long, Ack> record) {
                        LOGGER.debug("AckProcessor processing record {}", record);
                        var prevRecord = this.transactionStateStateStore.get(record.key());
                        var newRecord = new TransactionState();
                        newRecord.setId(record.key());
                        if (prevRecord == null) {
                            newRecord.setAckTs(record.value().getTs());
                            newRecord.setAckData(record.value().getAckData());
                            this.transactionStateStateStore.put(record.key(), newRecord);
                            LOGGER.info("New Ack id={}", record.key());
                        } else {
                            newRecord.setTransactionTs(prevRecord.getTransactionTs());
                            newRecord.setData(prevRecord.getData());
                            newRecord.setAckTs(record.value().getTs());
                            newRecord.setAckData(record.value().getAckData());
                            this.context.forward(new Record<>(newRecord.getId(), newRecord, record.timestamp()));
                            this.transactionStateStateStore.delete(record.key());
                            LOGGER.info("Found transaction for ack id={}", record.key());
                        }
                    }
                },
                TRANSACTION_STATE_STATESTORE
        ).to("output", Produced.with(Serdes.Long(), transactionStateSerde));

        return builder.build();
    }
}
