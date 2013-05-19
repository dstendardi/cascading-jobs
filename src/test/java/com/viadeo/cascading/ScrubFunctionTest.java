package com.viadeo.cascading;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class ScrubFunctionTest {

    @Test
    public void testScrubText() throws Exception {

        List<String> strings = Arrays.asList(
                "Java Script",
                "javascript",
                "JavaScript",
                "JavaScript",
                "JavaScript",
                "JavaScript",
                "java-script",
                "java_script",
                "jāvascript",
                "java/script",
                "java   script"
        );

        for(String string : strings) {
            assertEquals("javascript", ScrubFunction.scrubText(string));
        }
    }
}
