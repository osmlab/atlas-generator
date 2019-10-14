package org.openstreetmap.atlas.generator.tools.json;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.utilities.collections.Maps;

/**
 * @author lcram
 */
public class PersistenceJsonParserTest
{
    @Test
    public void testGetJsonKey()
    {
        final String jsonKey1 = PersistenceJsonParser.getJsonKey("USA", "1-2-3", "zz/");
        final String jsonKey2 = PersistenceJsonParser.getJsonKey("USA", "1-2-3",
                Maps.hashMap("scheme", "", "foo", "bar"));
        Assert.assertEquals(
                "{\"country\":\"USA\",\"shard\":\"1-2-3\",\"metadata\":{\"scheme\":\"zz/\"}}",
                jsonKey1);
        Assert.assertEquals(
                "{\"country\":\"USA\",\"shard\":\"1-2-3\",\"metadata\":{\"scheme\":\"\",\"foo\":\"bar\"}}",
                jsonKey2);
    }

    @Test
    public void testParse()
    {
        final String json = "{\"country\":\"USA\",\"shard\":\"1-2-3\",\"metadata\":{\"scheme\":\"zz/\"}}";

        final String country = PersistenceJsonParser.parseCountry(json);
        Assert.assertEquals("USA", country);

        final String shard = PersistenceJsonParser.parseShard(json);
        Assert.assertEquals("1-2-3", shard);

        final String scheme = PersistenceJsonParser.parseScheme(json);
        Assert.assertEquals("zz/", scheme);

        final Map<String, String> metadata = PersistenceJsonParser.parseMetadata(json);
        Assert.assertEquals(Maps.hashMap("scheme", "zz/"), metadata);
    }
}
