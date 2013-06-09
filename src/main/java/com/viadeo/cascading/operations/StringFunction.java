package com.viadeo.cascading.operations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.Serializable;

public class StringFunction extends BaseOperation implements Function {

    private final String fieldName;
    private final StringOperation operation;

    public StringFunction(Fields fieldDeclaration, String fieldName, StringOperation operation) {
        super(fieldDeclaration);
        this.fieldName = fieldName;
        this.operation = operation;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tuple = functionCall.getArguments();

        Tuple result = new Tuple();
        result.add(operation.operate(tuple.getString(fieldName)));

        functionCall.getOutputCollector().add(result);
    }

    public interface StringOperation extends Serializable {
        String operate(String value);
    }
}
