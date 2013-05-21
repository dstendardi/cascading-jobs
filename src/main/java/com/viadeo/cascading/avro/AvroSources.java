package com.viadeo.cascading.avro;

import cascading.avro.AvroScheme;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class AvroSources {

    private List<Source> sources;

    private class Source  {
        public Pipe pipe;
        public Tap tap;

        Source (Pipe pipe, Tap tap) {
            this.pipe = pipe;
            this.tap = tap;
        }
    };

    /**
     * Build a source list from given class paths
     *
     * @param paths source files
     */
    public AvroSources(String... paths) {
        sources = Lists.newArrayList();
        for (String path : paths) {
            Source type = asSource(path);
            sources.add(type);
        }
    }

    /**
     * Transform source list into a map of tap
     * indexed by pipe name
     *
     * @return tap map
     */
    public HashMap<String, Tap> asMap() {

        HashMap<String, Tap> map = Maps.newHashMap();
        for(Source source : sources) {
            map.put(source.pipe.getName(), source.tap);
        }

        return map;
    }

    /**
     * Transform source list into a pipe array
     *
     * @return the pipes from the source list
     */
    public Pipe[] asPipes() {

        ArrayList<Pipe> pipes = Lists.newArrayList();
        for(Source source : sources) {
            pipes.add(source.pipe);
        }

        return pipes.toArray(new Pipe[pipes.size()]);
    }

    /**
     * Transform a file path into a source entry
     *
     * @param path the path of the source file
     * @return a source object
     */
    private Source asSource(String path) {

        if(path == null){
            throw new IllegalArgumentException("path cannot be null");
        }

        File file = new File(path);

        if(!(file.exists() && file.isFile())){
            throw new IllegalArgumentException("path should refer to an existing file");
        }

        String type = FilenameUtils.removeExtension(file.getName());

        Pipe pipe = new Pipe(type + ":path");
        pipe = new Each(pipe, new Fields("id", "term"), new Insert(new Fields("type"),  type), Fields.ALL);

        return new Source(pipe, new Lfs(new AvroScheme(), path));
    }
}
