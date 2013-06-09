package com.viadeo.cascading.steps;

import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.URL;
import java.util.List;
import java.util.Map;


public class HBaseSteps {

    private HBaseAdmin admin;
    private Configuration conf;
    private Map<String, HTable> tables;

    class Cell {
        String row;
        String family;
        String qualifier;
        Long timestamp;
        String value;
    }

    @Before
    public void setupHBase() throws Exception {
        conf = HBaseConfiguration.create();
        admin = new HBaseAdmin(conf);

        if (admin.tableExists("events")) {
            admin.disableTable("events");
            admin.deleteTable("events");
        }

        HTableDescriptor table = new HTableDescriptor("events");
        table.addFamily(new HColumnDescriptor("system"));
        table.addFamily(new HColumnDescriptor("application"));
        table.addFamily(new HColumnDescriptor("domain"));
        admin.createTable(table);

        tables = Maps.newHashMap();
        tables.put("events", new HTable("events"));
    }


    @Given("^a table \"([^\"]*)\" containing the following cells$")
    public void a_table_containing_the_following_cells(String tableName, List<Cell> cells) throws Throwable {

        for (Cell cell : cells) {
            Put put = new Put(Bytes.toBytes(cell.row));
            URL url = Resources.getResource(cell.value);
            put.add(Bytes.toBytes(cell.family), Bytes.toBytes(cell.qualifier), cell.timestamp, Resources.toByteArray(url));
            tables.get(tableName).put(put);
        }
    }
}
