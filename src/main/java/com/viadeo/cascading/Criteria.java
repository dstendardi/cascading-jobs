package com.viadeo.cascading;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
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
import com.google.common.base.CharMatcher;
import com.viadeo.cascading.avro.AvroSources;
import com.viadeo.cascading.operations.StringFunction;
import org.elasticsearch.hadoop.cascading.ESTap;

import java.io.IOException;
import java.text.Normalizer;
import java.util.Properties;

public class Criteria {


    public static void main(String[] args) throws IOException {

        // setup
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Criteria.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        AvroSources sources = new AvroSources(args);
        Tap outTap = new ESTap("criteria/main", new Fields("preferred", "normalized", "origin", "type"));

        Pipe merged = new Merge("normalizer", sources.asPipes());
        Pipe normalized = normalize(merged);
        Pipe count = new CountBy(normalized, new Fields("term", "normalized", "type"), new Fields("count"));
        Pipe max = max(count);
        Pipe join = join(count, max);

        FlowDef flowDef = FlowDef.flowDef()
                .addSources(sources.asMap())
                .addTailSink(join, outTap);

        // run the flow
        flowConnector.connect(flowDef).complete();
    }

    /**
     * Normalized terms received from merged sources pipes
     * The resulting normalized string will then be used to perform aggregation
     *
     * @param merged pipes containing [id, term, type]
     * @return normalized pipe containg [
     */
    private static Pipe normalize(Pipe merged) {

        // normalizer
        Fields scrubArguments = new Fields("id", "term", "type");

        return new Each(merged, scrubArguments, new StringFunction(new Fields("normalized"), "term", new StringFunction.StringOperation() {
            @Override
            public String operate(String value) {
                String text = Normalizer
                        .normalize(value, Normalizer.Form.NFD)
                        .replaceAll("[^\\p{ASCII}]", "");

                return CharMatcher.JAVA_LETTER.retainFrom(text).toLowerCase();
            }
        }), Fields.ALL);
    }

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
    private static Pipe join(Pipe count, Pipe max) {

        Pipe join = new CoGroup(
                count, new Fields("normalized", "type"),
                max, new Fields("normalized", "type"),
                new Fields("n_term", "n_normalized", "n_type", "n_count", "m_normalized", "m_type", "m_max"),
                new InnerJoin());
        join = new Retain(join, new Fields("n_term", "n_normalized", "n_type", "n_count", "m_max"));
        join = new Rename(join, new Fields("n_term", "n_normalized", "n_type", "n_count", "m_max"), new Fields("origin", "normalized", "type", "count", "max"));

        Fields transformArguments = new Fields("origin", "normalized", "type", "count", "max");
        join = new Each(join, transformArguments, new ExpressionFunction(new Fields("preferred"), "count == max", Integer.class), Fields.ALL);
        join = new Retain(join, new Fields("preferred", "origin", "normalized", "type"));

        return join;
    }

    /**
     * Create a field containing the maximum number of occurence
     * found for the given normalized value
     *
     * @param count pipe containing [normalized, type, count]
     * @return max pipe containing [normalized, type, count, max]
     */
    private static Pipe max(Pipe count) {
        Pipe max = new Pipe("max", count);
        max = new GroupBy(max, new Fields("normalized", "type"));
        max = new Every(max, new Fields("count"), new Max());
        return max;
    }
}