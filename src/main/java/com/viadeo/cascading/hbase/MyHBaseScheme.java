package com.viadeo.cascading.hbase;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.base.Throwables;
import com.twitter.maple.hbase.HBaseScheme;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

@SuppressWarnings({"deprecation"})
public class MyHBaseScheme extends HBaseScheme {


    private transient Class<? extends ScanTableInputFormat> tableInputFormat = ScanTableInputFormat.class;

    private transient Scan scan;

    public MyHBaseScheme(Scan scan, Fields rowkey, String system, Fields valueFields) {
        super(rowkey, system, valueFields);
        this.scan = scan;
    }

    public MyHBaseScheme(Class<? extends ScanTableInputFormat> inputFormat, Scan scan, Fields rowkey, String system, Fields valueFields) {
        super(rowkey, system, valueFields);
        this.scan = scan;
        this.tableInputFormat = inputFormat;
    }


    @Override
    @SuppressWarnings("unchecked")
    public void sourceConfInit(FlowProcess<JobConf> process,
                               Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

        super.sourceConfInit(process, tap, conf);
        conf.set(ScanTableInputFormat.SCAN_PROPERTY, convertScanToString(scan));
        conf.setInputFormat(tableInputFormat);
    }

    /**
     * Convert a Scan into a String representation
     *
     * @param scan a scan to use
     * @return a String
     */
    private String convertScanToString(Scan scan) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            DataOutputStream dos = new DataOutputStream(out);
            scan.write(dos);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return Base64.encodeBytes(out.toByteArray());
    }

}
