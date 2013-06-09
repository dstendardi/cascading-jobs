package com.viadeo.cascading;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.twitter.maple.hbase.HBaseTap;
import com.viadeo.cascading.hbase.EventTableInputFormat;
import com.viadeo.cascading.hbase.MyHBaseScheme;
import com.viadeo.cascading.operations.ExtractRow;
import com.viadeo.cascading.operations.FlattenLogFunction;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;


public class Migration {


    /**
     * A source config object allowing
     * to declare fields and callback
     * per sources.
     */
    static class SourceConfig {
        public Fields fields;
        public FlattenLogFunction.LogHandler callable;

        public SourceConfig(Fields fields, FlattenLogFunction.LogHandler callable) {
            this.fields = fields;
            this.callable = callable;
        }
    }

    /**
     * This map configure the different fields and callback needed to perform migration
     * The log handler take the map resulting from json deserialization and the tuple entry
     * to populate.
     */
    private static ImmutableMap<String, SourceConfig> fields = ImmutableMap.of(
            "http_access", new SourceConfig(
            new Fields("id", "path", "query", "protocol", "referer", "user-agent", "x-forwarded-for", "jsessionid", "rememberme", "status", "duration", "size", "content-type"),
            new FlattenLogFunction.LogHandler() {
                @Override
                @SuppressWarnings("unchecked")
                public void handle(Map<String, Object> log, Tuple result) {
                    result.add(log.get("id"));

                    Map<String, Object> request = (Map<String, Object>) log.get("request");

                    result.add(request.get("path"));
                    result.add(request.get("query"));
                    result.add(request.get("protocol"));

                    Map<String, Object> headers = (Map<String, Object>) request.get("headers");

                    result.add(headers.get("Referer"));
                    result.add(headers.get("User-Agent"));
                    result.add(headers.get("X-Forwarded-For"));

                    Map<String, Object> cookies = (Map<String, Object>) headers.get("Cookie");
                    result.add(cookies.get("JSESSIONID"));
                    result.add(cookies.get("rememberMe"));

                    Map<String, Object> response = (Map<String, Object>) log.get("response");

                    result.add(response.get("status"));
                    result.add(response.get("duration"));
                    result.add(response.get("size"));
                    result.add(response.get("Content-Type"));
                }
            }),
            "syslog", new SourceConfig(
            new Fields("facility", "severity", "message"),
            new FlattenLogFunction.LogHandler() {
                @Override
                @SuppressWarnings("unchecked")
                public void handle(Map<String, Object> log, Tuple result) {
                    result.add(log.get("facility"));
                    result.add(log.get("severity"));
                    result.add(log.get("message"));
                }
            })
    );


    /**
     * A job that take events from hbase and sinks to hdfs.
     * Some nice customization are down using TemplateTap in order to
     * split output into several directory, using tuple values
     *
     * @param args output directory
     * @throws IOException
     */
    public static void main(String... args) throws IOException {

        Preconditions.checkArgument(args.length == 4, "usage : <output> <start date> <end date> <comma separated prefixes>");

        Properties properties = new Properties();
        properties.put(EventTableInputFormat.PREFIXES, args[3]);
        AppProps.setApplicationJarClass(properties, Migration.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        // scan
        Scan scan = new Scan();
        scan.setCaching(10000);
        scan.setBatch(10000);
        scan.setCacheBlocks(false);
        scan.setTimeRange(Long.valueOf(args[1]), Long.valueOf(args[2]));

        Fields columns = Fields.VALUES
                .append(new Fields("p-api_access-json-1"))
                .append(new Fields("p-http_access-json-1"))
                .append(new Fields("p-syslog-json-1"));


        MyHBaseScheme myHBaseScheme = new MyHBaseScheme(EventTableInputFormat.class, scan, new Fields("rowkey"), "system", columns);

        Tap<JobConf, RecordReader, OutputCollector> in = new HBaseTap("events", myHBaseScheme, SinkMode.KEEP);
        Pipe copyPipe = new Pipe("copy");
        copyPipe = new Each(copyPipe, new ExtractRow());


        FlowDef flowDef = FlowDef.flowDef().addSource(copyPipe, in);
        for (Map.Entry<String, SourceConfig> entry : fields.entrySet()) {
            createOutput(args[0], entry.getKey(), entry.getValue(), copyPipe, flowDef);
        }

        flowConnector.connect(flowDef).complete();
    }


    /**
     * Dynamically creates an output for each log formats
     *
     * @param output       where to put our files
     * @param type         the type of logs (http_access, syslog)
     * @param sourceConfig the configuration containing fields and callback
     * @param copyPipe     input pipe
     * @param flowDef      flowdefinition to update
     */
    private static void createOutput(String output, String type, SourceConfig sourceConfig, Pipe copyPipe, FlowDef flowDef) {

        Fields fields = new Fields("h_id", "h_timestamp").append(sourceConfig.fields);

        Pipe pipe = new Pipe(type, copyPipe);

        String expression = String.format("!type.equals(\"%s\")", type);

        pipe = new Each(pipe, Fields.ALL, new ExpressionFilter(expression, String.class));
        pipe = new Each(pipe, new FlattenLogFunction(sourceConfig.fields, sourceConfig.callable));

        TextLine textLine = new TextLine();
        textLine.setSinkFields(fields);
        Tap<JobConf, Void, OutputCollector> out = new TemplateTap(new Hfs(textLine, output), "%s/%s/%s/%s", new Fields("h_type", "h_version", "h_year", "h_yearmonthday"));
        flowDef.addTailSink(pipe, out);
    }

}
