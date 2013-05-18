package com.viadeo.cascading.steps

import com.viadeo.cascading.Main
import cucumber.api.groovy.EN
import cucumber.api.groovy.Hooks

this.metaClass.mixin(Hooks)
this.metaClass.mixin(EN)

def File input
def File output
def File tmp

World {
    tmp = new File("/tmp/cucumber-test")
    input = new File(tmp, "input")
    output = new File(tmp, "output")
}

Before {
    tmp.deleteDir()
    tmp.mkdir()
}

Given(~'^a file containing the following lines$') { table ->
    table.raw().each {
        input <<  it.join("\t")  + "\n"
    }
}

When(~'^I run the "([^"]*)" job$') { String arg1 ->
    Main.main(input.getAbsolutePath(), output.getAbsolutePath())
}

Then(~'^the output file should contain the following lines$') { table ->
    assert new File(output, "_SUCCESS").exists()
    def result = new File(output, "part-00000")
    assert result.exists()
    lines = result.readLines()
    headers = lines.remove(0).split("\t")

    b = []
    lines.eachWithIndex  { it, index ->
        list = it.split("\t")
        given = [:]
        list.eachWithIndex { nit, nindex ->
            given[headers[nindex]] = nit
        }
        b << given
    }

    def a = table.asMaps()

    assert a == b
}