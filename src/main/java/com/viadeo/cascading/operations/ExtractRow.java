package com.viadeo.cascading.operations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class ExtractRow extends BaseOperation implements Function {

    public ExtractRow() {
        super(1, new Fields("salt", "pool", "source", "timestamp", "host", "id", "type", "format", "version", "payload"));
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry entry = functionCall.getArguments();
        Tuple result = new Tuple();
        Fields fields = entry.getFields();
        for (String part : getString(entry.getObject(0)).split("\\-")) {
            result.add(part);
        }

        int size = fields.size();

        for (int i = 1; i < size; i++) {
            Object value = entry.getObject(i);
            String payload = getString(value);
            if (payload.length() > 0) {
                String field = (String) fields.get(i);
                String[] split = field.split("\\-");
                int u = 0;
                for (String part : split) {
                    if (u > 0) {
                        result.add(part);
                    }
                    u++;
                }
                result.add(getString(entry.getObject(i)));
            }
        }

        functionCall.getOutputCollector().add(result);
    }

    private String getString(Object value) {
        byte[] bytes = ((ImmutableBytesWritable) value).get();
        return new String(bytes);
    }

}