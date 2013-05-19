package com.viadeo.cascading;

import java.util.Properties;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Max;
import cascading.pipe.*;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.elasticsearch.hadoop.cascading.ESTap;

public class Main {

    public static void main(String[] args) {

        // setup
        String inPath = args[0];
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        // create the source tap
        Tap inTap = new Hfs(new TextDelimited(true, "\t"), inPath);
        Tap outTap = new ESTap("criteria/skills", new Fields("preferred", "normalized"));

        // normalizer
        Pipe normalizer = new Pipe("normalizer");
        Fields scrubArguments = new Fields("id", "term");
        normalizer = new Each(normalizer, scrubArguments, new ScrubFunction(scrubArguments), Fields.RESULTS);

        // count
        normalizer = new GroupBy(normalizer, new Fields("term", "normalized"));
        normalizer = new Every(normalizer, Fields.ALL, new Count(), Fields.ALL);

        // max
        Pipe max = new Pipe("max", normalizer);
        max = new GroupBy(max, new Fields("normalized"));
        max = new Every(max, new Fields("count"), new Max());

        // keep only preferred variants
        Pipe join = new CoGroup(normalizer, new Fields("normalized", "count"), max, new Fields("normalized", "max"), new Fields("a", "b", "c", "d", "e"), new InnerJoin());
        join = new Retain(join, new Fields("a", "b"));
        join = new Rename(join, new Fields("a", "b"), new Fields("preferred", "normalized"));

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(normalizer, inTap)
                .addTailSink(join, outTap);

        // run the flow
        flowConnector.connect(flowDef).complete();
    }
}
