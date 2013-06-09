package com.viadeo.cascading.hbase;

import com.twitter.maple.hbase.mapred.TableInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

@SuppressWarnings("deprecation")
public class ScanTableInputFormat extends TableInputFormat {

    public static final String SCAN_PROPERTY = "hbase.mapreduce.scan";

    private final Log LOG = LogFactory.getLog(ScanTableInputFormat.class);

    private Scan scan;


    protected void setScan(Scan scan) {
        this.scan = scan;
    }


    /**
     * Converts the given Base64 string back into a Scan instance.
     *
     * @param conf The job configuration
     * @return The newly created Scan instance.
     * @throws IOException When reading the scan instance fails.
     */
    protected Scan getScan(JobConf conf) throws IOException {

        if (null != scan) {
            return scan;
        }

        String serializedScan = conf.get(ScanTableInputFormat.SCAN_PROPERTY);

        ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(serializedScan));
        DataInputStream dis = new DataInputStream(bis);
        scan = new Scan();
        scan.readFields(dis);

        return scan;
    }


    /**
     * Calculates the splits that will serve as input for the map tasks. The
     * number of splits matches the number of regions in a table.
     *
     * @param conf      the map task {@link JobConf}
     * @param numSplits is not used anymore, but here for retrocompatibility
     * @return The list of input splits.
     * @throws IOException When creating the list of splits fails.
     * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
     *org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {

        Scan scan = getScan(conf);

        if (getHTable() == null) {
            throw new IOException("No table was provided.");
        }
        Pair<byte[][], byte[][]> keys = getHTable().getStartEndKeys();
        if (keys == null || keys.getFirst() == null ||
                keys.getFirst().length == 0) {
            throw new IOException("Expecting at least one region.");
        }
        int count = 0;

        InputSplit[] splits = new InputSplit[keys.getFirst().length];
        for (int i = 0; i < keys.getFirst().length; i++) {
            String regionLocation = getHTable().getRegionLocation(keys.getFirst()[i]).
                    getHostname();
            byte[] startRow = scan.getStartRow();
            byte[] stopRow = scan.getStopRow();
            // determine if the given start an stop key fall into the region
            if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
                    Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
                    (stopRow.length == 0 ||
                            Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
                byte[] splitStart = startRow.length == 0 ||
                        Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
                        keys.getFirst()[i] : startRow;
                byte[] splitStop = (stopRow.length == 0 ||
                        Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
                        keys.getSecond()[i].length > 0 ?
                        keys.getSecond()[i] : stopRow;
                InputSplit split = new TableSplit(getHTable().getTableName(),
                        splitStart, splitStop, regionLocation);
                splits[i] = split;
                if (LOG.isDebugEnabled())
                    LOG.debug("getSplits: split -> " + (count++) + " -> " + split);
            }
        }
        return splits;
    }
}
