package org.fred.demo.ooo;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

public class AckProcessor implements Processor<Long, Ack, Long, TransactionState> {

    private static final Logger LOGGER = LogManager.getLogger();

    private ProcessorContext<Long, TransactionState> context;
    private KeyValueStore<Long, TransactionState> transactionStateStateStore;

    @Override
    public void init(ProcessorContext<Long, TransactionState> context) {
        Processor.super.init(context);
        this.context = context;
        this.transactionStateStateStore = context.getStateStore(KsAppOOO.TRANSACTION_STATE_STATESTORE);
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

}
