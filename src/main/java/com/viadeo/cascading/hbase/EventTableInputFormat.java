package com.viadeo.cascading.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EventTableInputFormat extends ScanTableInputFormat {

    public static final String PREFIXES = "hbase.mapreduce.scan.prefixes";
    private final Logger LOG = LoggerFactory.getLogger(EventTableInputFormat.class);

    public static final String PATTERN = "%d-%s-%s";

    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {


        List<InputSplit> allSplits = new ArrayList<InputSplit>();
        Scan originalScan = getScan(conf);
        Iterable<String> prefixes = getPrefixes(conf);

        List<Integer> salts = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        TimeRange timeRange = originalScan.getTimeRange();

        List<Scan> scans = new ArrayList<Scan>();
        for (int salt : salts) {
            for (String prefix : prefixes) {
                String start = String.format(PATTERN, salt, prefix, timeRange.getMin());
                String end = String.format(PATTERN, salt, prefix, timeRange.getMax());
                scans.add(new Scan(start.getBytes(), end.getBytes()));
                LOG.info("adding scan with start = {}, end = {}", start, end);
            }
        }

        for (Scan scan : scans) {
            // Internally super.getSplits(...) uses scan object stored in private variable,
            // to re-use the code of super class we switch scan object with scans we
            setScan(scan);
            InputSplit[] splits = super.getSplits(conf, numSplits);
            allSplits.addAll(Lists.newArrayList(splits));
        }

        // Setting original scan back
        setScan(originalScan);

        return allSplits.toArray(new InputSplit[allSplits.size()]);
    }

    private Iterable<String> getPrefixes(JobConf conf) {
        String pref = conf.get(PREFIXES);
        Preconditions.checkState(!Strings.isNullOrEmpty(pref), "You must declare the property " + PREFIXES + " inside your hadoop configuration when using EventTableInputFormat");
        Iterable<String> split = Splitter.on(",").omitEmptyStrings().trimResults().split(pref);
        Preconditions.checkState(!Iterables.isEmpty(split), "You must provide at least one prefix in order to scan event table");

        return split;
    }

}