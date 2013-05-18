package com.viadeo.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.base.CharMatcher;

import java.text.Normalizer;

public class ScrubFunction extends BaseOperation implements Function {

    public ScrubFunction(Fields fieldDeclaration) {
        super(2, fieldDeclaration);
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry argument = functionCall.getArguments();
        String id = argument.getString("id");
        String term = scrubText(argument.getString("term"));
        if (term.length() > 0) {
            Tuple result = new Tuple();
            result.add(id);
            result.add(term);
            functionCall.getOutputCollector().add(result);
        }
    }

    public String scrubText(String text) {

        text = Normalizer
                .normalize(text, Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", "");

        text = CharMatcher.JAVA_LETTER.retainFrom(text).toLowerCase();

        return text;
    }
}