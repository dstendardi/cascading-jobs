package com.viadeo.cascading;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.*;
import cascading.operation.aggregator.Max;
import cascading.pipe.*;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.base.Preconditions;
import com.viadeo.cascading.avro.AvroSources;
import com.viadeo.cascading.operations.StringFunction;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.elasticsearch.hadoop.cascading.ESTap;

import java.io.IOException;
import java.text.Normalizer;
import java.util.Iterator;
import java.util.Properties;

public class Criteria {

    static public class SomeBuffer extends BaseOperation implements Buffer
    {


        public SomeBuffer(Fields fields) {
            super(fields);
        }

        public void operate( FlowProcess flowProcess, BufferCall bufferCall )
        {
            // get the group values for the current grouping
            TupleEntry group = bufferCall.getGroup();

            // get all the current argument values for this grouping
            Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

            final TObjectLongHashMap<String> map = new TObjectLongHashMap<String>();

            String preferred = "";
            long max = 0L;
            while( arguments.hasNext() )
            {
                TupleEntry argument = arguments.next();
                String term = argument.getString("term");
                long test = map.adjustOrPutValue(term, 1, 1);

                if (max < test) {
                    max = test;
                    preferred = term;
                }
            }

            for(Object term : map.keys()) {
                bufferCall.getOutputCollector().add(new Tuple(preferred.equals(term), term, group.getString("type"), group.getString("normalized")));
            }
        }
    }


    public static void main(String[] args) throws IOException {

        // setup
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Criteria.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        AvroSources sources = new AvroSources(args);
        Tap outTap = new ESTap("criteria/main");
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



        Fields fields = new Fields("a", "b", "c", "d");
        Pipe aggregate = new GroupBy(normalized, new Fields("type", "normalized"));
        aggregate = new Every(aggregate, new SomeBuffer(fields), fields);
        aggregate = new Retain(aggregate, fields);
        aggregate = new Rename(aggregate, fields, new Fields("preferred", "origin", "type", "normalized"));


        FlowDef flowDef = FlowDef.flowDef()
                .addSources(sources.asMap())
                .addTailSink(aggregate, outTap);

        // run the flow
        flowConnector.connect(flowDef).complete();
    }
}