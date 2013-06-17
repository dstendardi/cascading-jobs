package com.viadeo.cascading;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Max;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.*;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.viadeo.cascading.avro.AvroSources;
import com.viadeo.cascading.operations.StringFunction;
import org.elasticsearch.hadoop.cascading.ESTap;

import java.io.IOException;
import java.text.Normalizer;
import java.util.Iterator;
import java.util.Properties;

public class Criteria {


    public static void main(String[] args) throws IOException {

        // setup
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Criteria.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        AvroSources sources = new AvroSources(args);
        Tap outTap = new ESTap("criteria/main", new Fields("id",   "preferred", "normalized", "origin", "type"));
        //Pipe merged = new GroupBy(sources.asPipes(), new Fields("term", "type"));
        Pipe merged = new Merge("normalizer", sources.asPipes());


        /**
         * Normalized terms received from merged sources pipes
         * The resulting normalized string will then be used to perform aggregation
         *
         * @param merged pipes containing [id, term, type]
         * @return normalized pipe containg [
         */
        Fields scrubArguments = new Fields("id", "term", "type");
        Pipe normalized = new Each(merged, scrubArguments, new StringFunction(new Fields("normalized"), "term", new StringFunction.StringOperation() {
            @Override
            public String operate(String value) {
                String text = Normalizer
                        .normalize(value, Normalizer.Form.NFD)
                        .replaceAll("[^\\p{ASCII}]", "");

                return text.toLowerCase().trim();
            }
        }), Fields.ALL);

        Pipe count = new CountBy(normalized, new Fields("term", "normalized", "type"), new Fields("count"));


        /**
         * Create a field containing the maximum number of occurence
         * found for the given normalized value
         *
         * @param count pipe containing [normalized, type, count]
         * @return max pipe containing [normalized, type, count, max]
         */
        Pipe max = new Pipe("max", count);
        max = new GroupBy(max, new Fields("normalized", "type"));
        max = new Every(max, new Fields("count"), new Max());

        /**
         * Joins the count pipe and the max pipe
         * <p/>
         * As we have duplicate fields between this two pipes (normalized, type)
         * we declare output fields names prefixed by the pipe first letter (n, m)
         * <p/>
         * We then retain and rename the following fields
         * [origin, normalized, type, count, max]
         * <p/>
         * Then we create a new boolean column named preferred with a value resulting from an expression
         *
         * @param count pipe containing [term, normalized, type, count]
         * @param max   pipe containing [normalized, type, max]
         * @return joined pipe containing [preferred, origin, normalized, type]
         */
        Pipe join = new CoGroup(
                count, new Fields("normalized", "type"),
                max, new Fields("normalized", "type"),
                new Fields("n_term", "n_normalized", "n_type", "n_count", "m_normalized", "m_type", "m_max"),
                new InnerJoin());
        join = new Retain(join, new Fields("n_term", "n_normalized", "n_type", "n_count", "m_max"));
        join = new Rename(join, new Fields("n_term", "n_normalized", "n_type", "n_count", "m_max"), new Fields("origin", "normalized", "type", "count", "max"));

        Fields transformArguments = new Fields("origin", "normalized", "type", "count", "max");
        Fields output = new Fields("preferred", "origin", "normalized", "type");
        join = new Each(join, transformArguments, new ExpressionFunction(new Fields("preferred"), "count == max", Long.class), Fields.ALL);
        join = new Retain(join, output);

        // create id
        join = new Each(join, output, new ConcatenateFunction(new Fields("id"), new Fields("origin", "type")), new Fields("id", "preferred", "origin", "normalized", "type"));

        FlowDef flowDef = FlowDef.flowDef()
                .addSources(sources.asMap())
                .addTailSink(join, outTap);

        // run the flow
        flowConnector.connect(flowDef).complete();
    }


    public static class ConcatenateFunction extends BaseOperation implements Function {

        /** */
        private static final long serialVersionUID = 3833183264923340075L;
        private final Fields fields;

        public ConcatenateFunction(final Fields field, final Fields refFields){
            super(1, field);
            Preconditions.checkArgument(refFields.size() >= 2, "need at least 2 arguments to concatenate");
            this.fields = refFields;
        }

        @Override
        public void operate(final FlowProcess flowProcess, final FunctionCall functionCall) {
            final TupleEntry tuple = functionCall.getArguments();
            final Tuple result = new Tuple();
            result.add(concat(tuple));
            functionCall.getOutputCollector().add(result);
        }

        private String concat(final TupleEntry tuple) {
            final StringBuilder sb = new StringBuilder();
            final Iterator iterator = fields.iterator();
            while(iterator.hasNext()){
                if(sb.length() > 0){
                    sb.append("_");
                }
                sb.append(tuple.getString(String.valueOf(iterator.next())));
            }
            return sb.toString();
        }
    }
}