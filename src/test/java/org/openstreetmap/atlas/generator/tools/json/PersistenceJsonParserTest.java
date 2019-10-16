package org.openstreetmap.atlas.generator.tools.json;

import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.utilities.collections.Maps;

/**
 * @author lcram
 */
public class PersistenceJsonParserTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetJsonKey()
    {
        final String jsonKey1 = PersistenceJsonParser.createJsonKey("USA", "1-2-3", "zz/");
        final String jsonKey2 = PersistenceJsonParser.createJsonKey("USA", "1-2-3",
                Maps.hashMap("scheme", "", "foo", "bar"));
        Assert.assertEquals(
                "{\"country\":\"USA\",\"shard\":\"1-2-3\",\"metadata\":{\"scheme\":\"zz/\"}}",
                jsonKey1);
        Assert.assertEquals(
                "{\"country\":\"USA\",\"shard\":\"1-2-3\",\"metadata\":{\"scheme\":\"\",\"foo\":\"bar\"}}",
                jsonKey2);
    }

    @Test
    public void testMissingRequiredKey()
    {
        final String json = "{\"foo\":\"bar\",\"shard\":\"1-2-3\",\"metadata\":{\"scheme\":\"zz/\"}}";
        this.expectedException.expect(CoreException.class);
        this.expectedException.expectMessage("Property \"country\" not found in JSON object");
        PersistenceJsonParser.parseCountry(json);
    }

    @Test
    public void testParse()
    {
        final String json = "{\"country\":\"USA\",\"shard\":\"1-2-3\",\"metadata\":{\"scheme\":\"zz/\"}}";
        final String json2 = PersistenceJsonParser.createJsonKey("USA", "4-5-6",
                Maps.hashMap("foo", "bar"));

        final String country = PersistenceJsonParser.parseCountry(json);
        Assert.assertEquals("USA", country);

        final String shard = PersistenceJsonParser.parseShard(json);
        Assert.assertEquals("1-2-3", shard);

        final Optional<String> scheme = PersistenceJsonParser.parseScheme(json);
        Assert.assertTrue(scheme.isPresent());
        Assert.assertEquals("zz/", scheme.get());

        final Map<String, String> metadata = PersistenceJsonParser.parseMetadata(json);
        Assert.assertEquals(Maps.hashMap("scheme", "zz/"), metadata);

        final Optional<String> scheme2 = PersistenceJsonParser.parseScheme(json2);
        Assert.assertFalse(scheme2.isPresent());
    }
}
