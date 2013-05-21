package com.viadeo.cascading.steps;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import cucumber.api.DataTable;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.hamcrest.Matchers;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

public class MainSteps {

    public static final String INDEX_NAME = "criteria";
    public static final String INDEX_TYPE = "main";

    private List<String> paths;
    private Client client;
    public static final URL RESOURCE = Resources.getResource("elasticsearch/index.json");
    private File tmp;
    private Schema schema;

    @Before
    public void setUp() throws Exception {
        tmp = Files.createTempDir();

        paths = Lists.newArrayList();

        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        IndicesAdminClient indices = client.admin().indices();

        if (indices.prepareExists(INDEX_NAME).execute().actionGet().exists()) {
            indices.prepareDelete(INDEX_NAME).execute().actionGet();
        }
        indices.prepareCreate(INDEX_NAME).setSource(Resources.toByteArray(RESOURCE)).execute().actionGet();

        schema = new Schema.Parser().parse(new File(Resources.getResource("com/viadeo/cascading/avro/criteria.avsc").getPath()));
    }

    @After
    public void clean() throws IOException {
        FileUtils.deleteDirectory(tmp);
    }

    @Given("^a file containing the following '([^\"]*)'$")
    public void a_file_containing_the_following_criterias(String criteria, DataTable table) throws Throwable {
        File input = new File(tmp, criteria + ".avro");

        ReflectDatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<GenericRecord>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.snappyCodec());
        dataFileWriter.create(schema, input);

        for (Map m : table.asMaps()) {
            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
            for (Schema.Field field : schema.getFields()) {
                recordBuilder.set(field, m.get(field.name()));
            }
            dataFileWriter.append(recordBuilder.build());
        }

        dataFileWriter.close();

        paths.add(input.getAbsolutePath());
    }

    @When("^I run the \"([^\"]*)\" job$")
    public void I_run_given_job(String job) throws Throwable {
        Class<?> clazz = Class.forName("com.viadeo.cascading." + job);
        String[] arguments = this.paths.toArray(new String[this.paths.size()]);
        clazz.getMethod("main", new Class[]{String[].class}).invoke(clazz, new Object[]{arguments});
    }

    @Then("^the output file should contain the following lines$")
    public void the_output_file_should_contain_the_following_lines(final List<Row> expected) throws Throwable {
        SearchResponse response = client.prepareSearch(INDEX_NAME)
                .setTypes(INDEX_TYPE)
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.fieldSort("origin"))
                .setSize(100)
                .execute().actionGet();

        List<Row> actual = Lists.newArrayList();

        for (SearchHit hit : response.hits().getHits()) {
            actual.add(createRow(hit.getSource()));
        }

        assertThat(actual, Matchers.equalTo(expected));
    }

    private Row createRow(Map<String, Object> source) {
        Row row = new Row();
        row.normalized = String.valueOf(source.get("normalized"));
        row.preferred = Boolean.valueOf(String.valueOf(source.get("preferred")));
        row.origin = String.valueOf(source.get("origin"));
        row.type = String.valueOf(source.get("type"));
        return row;
    }

    private static class Row {
        public String normalized;
        public Boolean preferred;
        public String origin;
        public String type;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Row)) {
                return false;
            }
            Row that = (Row) o;
            return Objects.equal(this.normalized, that.normalized) &&
                    Objects.equal(this.type, that.type) &&
                    Objects.equal(this.preferred, that.preferred) &&
                    Objects.equal(this.origin, that.origin);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(
                    normalized,
                    type,
                    preferred,
                    origin
            );
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("normalized", normalized)
                    .add("preferred", preferred)
                    .add("origin", origin)
                    .add("type", type)
                    .toString();
        }
    }
}
