package org.openstreetmap.atlas.generator.tools.json;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.utilities.collections.Maps;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Define a JSON format for the RDD keys used by the spark job. This class provides methods for
 * constructing the key as well as parsing data out of the key. The JSON format should follow this
 * structure:
 * 
 * <pre>
 * {
 *    "country": "USA-0-generated",
 *    "shard": "1-2-3",
 *    "metadata": {
 *         "scheme": "zz/",
 *    }
 * }
 * </pre>
 * 
 * @author lcram
 */
public final class PersistenceJsonParser
{
    private static final String COUNTRY_KEY = "country";
    private static final String SHARD_KEY = "shard";
    private static final String METADATA_KEY = "metadata";
    private static final String SCHEME_KEY = "scheme";

    /**
     * Get a JSON key with a given country and shard. Automatically populate a metadata object with
     * some given scheme information.
     * 
     * @param country
     *            the country, usually the ISO3 country code as well as possible creative additions
     * @param shard
     *            the shard name, i.e. the result of {@link Shard#getName}
     * @param scheme
     *            the scheme string, e.g. zz/
     * @return a JSON key with the given elements
     */
    public static String getJsonKey(final String country, final String shard, final String scheme)
    {
        return PersistenceJsonParser.getJsonKey(country, shard, Maps.hashMap(SCHEME_KEY, scheme));
    }

    public static String getJsonKey(final String country, final String shard,
            final Map<String, String> metadata)
    {
        final JsonObject jsonKey = new JsonObject();
        jsonKey.addProperty(COUNTRY_KEY, country);
        jsonKey.addProperty(SHARD_KEY, shard);

        final JsonObject metadataObject = new JsonObject();
        for (final Map.Entry<String, String> entry : metadata.entrySet())
        {
            metadataObject.addProperty(entry.getKey(), entry.getValue());
        }

        jsonKey.add(METADATA_KEY, metadataObject);
        return jsonKey.toString();
    }

    public static String parseCountry(final String json)
    {
        return parseStringProperty(json, COUNTRY_KEY);
    }

    public static Map<String, String> parseMetadata(final String json)
    {
        final JsonParser parser = new JsonParser();
        final JsonObject parsedObject = parser.parse(json).getAsJsonObject();
        final JsonObject metadataObject = parsedObject.get(METADATA_KEY).getAsJsonObject();
        final Map<String, String> map = new HashMap<>();
        for (final Map.Entry<String, JsonElement> entry : metadataObject.entrySet())
        {
            final String key = entry.getKey();
            final String value = entry.getValue().getAsString();
            map.put(key, value);
        }

        return map;
    }

    public static String parseScheme(final String json)
    {
        final JsonParser parser = new JsonParser();
        final JsonObject parsedObject = parser.parse(json).getAsJsonObject();
        final JsonObject metadataObject = parsedObject.get(METADATA_KEY).getAsJsonObject();
        return metadataObject.get(SCHEME_KEY).getAsString();
    }

    public static String parseShard(final String json)
    {
        return parseStringProperty(json, SHARD_KEY);
    }

    private static String parseStringProperty(final String json, final String property)
    {
        final JsonParser parser = new JsonParser();
        final JsonObject parsedObject = parser.parse(json).getAsJsonObject();
        return parsedObject.get(property).getAsString();
    }

    private PersistenceJsonParser()
    {
    }
}
