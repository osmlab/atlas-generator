package org.openstreetmap.atlas.generator.tools.json;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.openstreetmap.atlas.exception.CoreException;
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
     * Create a JSON key with a given country and shard. Automatically populate a metadata object
     * with some given scheme information.
     * 
     * @param country
     *            the country, usually the ISO3 country code as well as possible creative additions
     * @param shard
     *            the shard name, i.e. the result of {@link Shard#getName}
     * @param scheme
     *            the scheme string, e.g. zz/
     * @return a JSON key with the given elements
     */
    public static String createJsonKey(final String country, final String shard,
            final String scheme)
    {
        return PersistenceJsonParser.createJsonKey(country, shard,
                Maps.hashMap(SCHEME_KEY, scheme));
    }

    /**
     * Create a JSON key with a given country and shard. Also, provide a string to string
     * {@link Map} containing the desired metadata.
     *
     * @param country
     *            the country, usually the ISO3 country code as well as possible creative additions
     * @param shard
     *            the shard name, i.e. the result of {@link Shard#getName}
     * @param metadata
     *            a string to string {@link Map} containing the metadata
     * @return a JSON key with the given elements
     */
    public static String createJsonKey(final String country, final String shard,
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

    /**
     * Given a valid JSON key, parse and return the value of the "country" property.
     *
     * @param json
     *            the JSON key
     * @return the value of the country property
     */
    public static String parseCountry(final String json)
    {
        return parseStringProperty(json, COUNTRY_KEY);
    }

    /**
     * Given a valid JSON key, parse and return the "metadata" property as a string to string
     * {@link Map}.
     *
     * @param json
     *            the JSON key
     * @return the value of the metadata property
     */
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

    /**
     * Given a valid JSON key, parse and return the value of the "scheme" property within the
     * "metadata" object, if present. Otherwise, return an empty {@link Optional}.
     *
     * @param json
     *            the JSON key
     * @return the value of the scheme property if present, otherwise an empty {@link Optional}
     */
    public static Optional<String> parseScheme(final String json)
    {
        final JsonParser parser = new JsonParser();
        final JsonObject parsedObject = parser.parse(json).getAsJsonObject();
        final JsonObject metadataObject = parsedObject.get(METADATA_KEY).getAsJsonObject();
        if (metadataObject.get(SCHEME_KEY) == null)
        {
            return Optional.empty();
        }
        return Optional.ofNullable(metadataObject.get(SCHEME_KEY).getAsString());
    }

    /**
     * Given a valid JSON key, parse and return the value of the "shard" property.
     *
     * @param json
     *            the JSON key
     * @return the value of the shard property
     */
    public static String parseShard(final String json)
    {
        return parseStringProperty(json, SHARD_KEY);
    }

    private static String parseStringProperty(final String json, final String property)
    {
        final JsonParser parser = new JsonParser();
        final JsonObject parsedObject = parser.parse(json).getAsJsonObject();
        if (parsedObject.get(property) == null)
        {
            throw new CoreException("Property \"{}\" not found in JSON object {}", property, json);
        }
        return parsedObject.get(property).getAsString();
    }

    private PersistenceJsonParser()
    {
    }
}
