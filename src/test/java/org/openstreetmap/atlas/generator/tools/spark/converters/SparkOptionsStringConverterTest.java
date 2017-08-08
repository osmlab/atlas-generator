package org.openstreetmap.atlas.generator.tools.spark.converters;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author matthieun
 */
public class SparkOptionsStringConverterTest
{
    @Test
    public void testMissingOptions()
    {
        final String options = "a->b,c=";
        final Map<String, String> expected = new HashMap<>();
        expected.put("a", "b");
        expected.put("c", "");
        final SparkOptionsStringConverter converter = new SparkOptionsStringConverter();
        final Map<String, String> result = converter.convert(options);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testMixedOptions()
    {
        final String options = "a->b,c=d";
        final Map<String, String> expected = new HashMap<>();
        expected.put("a", "b");
        expected.put("c", "d");
        final SparkOptionsStringConverter converter = new SparkOptionsStringConverter();
        final Map<String, String> result = converter.convert(options);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testOptionsWithOtherEquals()
    {
        final String options = "a->b,c=d=e";
        final Map<String, String> expected = new HashMap<>();
        expected.put("a", "b");
        expected.put("c", "d=e");
        final SparkOptionsStringConverter converter = new SparkOptionsStringConverter();
        final Map<String, String> result = converter.convert(options);
        Assert.assertEquals(expected, result);
    }
}
