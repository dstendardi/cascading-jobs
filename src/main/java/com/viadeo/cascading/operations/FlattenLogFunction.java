package com.viadeo.cascading.operations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class FlattenLogFunction extends BaseOperation implements Function {

    public static final String DEFAULT_ENVIRONMENT = "production";
    private final LogHandler handler;
    private ObjectMapper objectMapper;

    public interface LogHandler extends Serializable {
        public void handle(Map<String, Object> log, Tuple result);
    }


    public FlattenLogFunction(Fields fields, LogHandler handler) {
        super(9, new Fields("h_environment", "h_type", "h_version", "h_timestamp", "h_id", "h_year", "h_yearmonthday").append(fields));
        this.handler = handler;
    }


    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry arguments = functionCall.getArguments();

        DateTime timestamp = new DateTime(arguments.getLong("timestamp"));
        String id = arguments.getString("id");
        String type = arguments.getString("type");
        Integer version = arguments.getInteger("version");
        String payload = arguments.getString("payload");

        try {
            Tuple result = new Tuple();
            result.add(DEFAULT_ENVIRONMENT);
            result.add(type);
            result.add(version);
            result.add(timestamp.getMillis());
            result.add(id);
            result.add(timestamp.getYear());
            result.add(timestamp.toString(ISODateTimeFormat.date()));

            TypeReference<HashMap<String, Object>> typeRef
                    = new TypeReference<
                    HashMap<String, Object>
                    >() {
            };

            Map<String, Object> log = objectMapper.readValue(payload, typeRef);
            this.handler.handle(log, result);

            functionCall.getOutputCollector().add(result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}