package com.viadeo.cascading.steps;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import cucumber.api.DataTable;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Then;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class HdfsSteps {

    private File tempDir;

    @Then("^output \"([^\"]*)\" should contains the following tab separated values$")
    public void output_should_contains_the_following_tab_separated_values(String output, DataTable table) throws Throwable {

        String path = tempDir.getAbsolutePath().concat(output);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path filenamePath = new Path(path);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(filenamePath, true);


        List<List<String>> given = Lists.newArrayList();
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            if (next.isDirectory()) {
                continue;
            }
            InputStream in = fs.open(next.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            line = br.readLine();
            while (line != null) {
                given.add(Arrays.asList(line.split("\t")));
                line = br.readLine();
            }
        }
        assertEquals(table.raw(), given);
    }

    @And("^a target output directory$")
    public void a_target_output_directory() throws Throwable {
        tempDir = Files.createTempDir();
    }

    public File getTmpDirectory() {
        return tempDir;
    }
}
