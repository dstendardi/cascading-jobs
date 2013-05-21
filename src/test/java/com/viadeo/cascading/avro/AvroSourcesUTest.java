package com.viadeo.cascading.avro;


import cascading.avro.AvroScheme;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AvroSourcesUTest {

    private static File tmp;

    @Before
    public void setUp(){
        tmp = Files.createTempDir();
    }

    @After
    public void clean() throws IOException {
        FileUtils.deleteDirectory(tmp);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createSource_withoutSpecifiedPath_shouldThrowException(){
        new AvroSources(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createSource_withUnknownPath_shouldThrowException(){
        new AvroSources("plop");
    }

    @Test
    public void asMap() throws IOException {

        File one = createFile("one");
        File two = createFile("two");

        //when
        AvroSources sources = new AvroSources(one.getAbsolutePath(), two.getAbsolutePath());


        Map<String, Tap> expect = new HashMap<String, Tap>();
        expect.put("one:path", new Lfs(new AvroScheme(), one.getPath()));
        expect.put("two:path", new Lfs(new AvroScheme(), two.getPath()));

        //then
        assertEquals(expect, sources.asMap());
    }

    @Test
    public void asPipes() throws IOException {

        File one = createFile("one");
        File two = createFile("two");

        //when
        AvroSources sources = new AvroSources(one.getAbsolutePath(), two.getAbsolutePath());

        Pipe[] actual = sources.asPipes();

        assertEquals(2, actual.length);
        assertEquals("one:path", actual[0].getName());
        assertEquals("two:path", actual[1].getName());
    }


    protected File createFile(String name) throws IOException {
        // given
        File file = new File(tmp, name);
        if (!file.createNewFile()) {
            throw new RuntimeException("unable to write file");
        }
        return file;
    }

}
