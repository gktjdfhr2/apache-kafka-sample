package com.example.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class FilterProcessor implements Processor {

    private ProcessorContext context;

    @Override
    public void process(Record record) {
        if ( ((String)record.value()).length() > 5) {
            context.forward(record);
        }
        context.commit();
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
