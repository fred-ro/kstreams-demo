package org.fred.demo.ooo;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

public class TransactionProcessor implements Processor<Long, Transaction, Long, TransactionState> {

    private static final Logger LOGGER = LogManager.getLogger();

    private ProcessorContext<Long, TransactionState> context;
    private KeyValueStore<Long, TransactionState> transactionStateStateStore;

    @Override
    public void init(ProcessorContext<Long, TransactionState> context) {
        Processor.super.init(context);
        this.context = context;
        this.transactionStateStateStore = context.getStateStore(KsAppOOO.TRANSACTION_STATE_STATESTORE);
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

}
