package com.viadeo.cascading.steps

import com.viadeo.cascading.Main
import cucumber.api.groovy.EN
import cucumber.api.groovy.Hooks
import groovy.json.JsonBuilder
import io.searchbox.client.JestClient
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.ClientConfig
import io.searchbox.client.config.ClientConstants
import io.searchbox.core.Search
import io.searchbox.indices.DeleteIndex

this.metaClass.mixin(Hooks)
this.metaClass.mixin(EN)

def File input
def File output
def File tmp
def JestClient client

World {
    tmp = new File("/tmp/cucumber-test")
    input = new File(tmp, "input")
    output = new File(tmp, "output")

    // Configuration
    ClientConfig clientConfig = new ClientConfig();
    LinkedHashSet<String> servers = new LinkedHashSet<String>();
    servers.add("http://localhost:9200");
    clientConfig.getProperties().put(ClientConstants.SERVER_LIST, servers);
    clientConfig.getProperties().put(ClientConstants.IS_MULTI_THREADED, true);

    // Construct a new Jest client according to configuration via factory
    JestClientFactory factory = new JestClientFactory();
    factory.setClientConfig(clientConfig);
    client = factory.getObject();

}

Before {
    tmp.deleteDir()
    tmp.mkdir()
    client.execute(new DeleteIndex("criteria"))
}

Given(~'^a file containing the following lines$') { table ->
    table.raw().each {
        input << it.join("\t") + "\n"
    }
}

When(~'^I run the "([^"]*)" job$') { String arg1 ->
    Main.main(input.getAbsolutePath(), output.getAbsolutePath())
}

class Row {
    def normalized
    def preferred
}

Then(~'^the output file should contain the following lines$') { table ->
    table.asMaps().each { row ->
        def builder = new JsonBuilder()
        builder.query {
            term {
                normalized row.normalized
            }
        }
        def q = builder.toString()
        search = new Search(q)
        search.addIndex("criteria")
        search.addType("skills")

        def execute = client.execute(search)
        assert execute.isSucceeded()

        List<Row> list = execute.getSourceAsObjectList(Row.class)
        assert list.size() == 1
        assert list[0].preferred == row.preferred
    }
}