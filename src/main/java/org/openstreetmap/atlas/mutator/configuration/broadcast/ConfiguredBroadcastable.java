package org.openstreetmap.atlas.mutator.configuration.broadcast;

import java.io.Serializable;
import java.util.Map;

import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.parsing.AtlasMutatorConfigurationParser;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;

import com.google.gson.JsonObject;

/**
 * @author matthieun
 */
public abstract class ConfiguredBroadcastable implements Serializable, Broadcastable
{
    public static final String CONFIGURATION_ROOT = AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL
            + ".broadcast";
    public static final String CONFIGURATION_BROADCASTABLE_DEFINITION = "definition";

    public static final String TYPE_JSON_PROPERTY_VALUE = "_broadcastable";
    public static final String NAME_JSON_PROPERTY = "name";
    public static final String DEFINITION_JSON_PROPERTY = "definition";

    private static final long serialVersionUID = -1489086691317053394L;

    private final String name;
    private final String definition;

    public ConfiguredBroadcastable(final String name, final Configuration configuration)
    {
        this.name = name;
        final String root = CONFIGURATION_ROOT + "." + name;
        final ConfigurationReader reader = new ConfigurationReader(root);
        this.definition = reader.configurationValue(configuration,
                CONFIGURATION_BROADCASTABLE_DEFINITION);
    }

    public String getDefinition()
    {
        return this.definition;
    }

    public String getName()
    {
        return this.name;
    }

    public Object read(final Map<String, String> configuration)
    {
        return read(this.definition, configuration);
    }

    public JsonObject toJson()
    {
        final JsonObject broadcastableObject = new JsonObject();
        broadcastableObject.addProperty(AtlasMutatorConfiguration.TYPE_JSON_PROPERTY,
                TYPE_JSON_PROPERTY_VALUE);
        broadcastableObject.addProperty(NAME_JSON_PROPERTY, this.name);
        broadcastableObject.addProperty(DEFINITION_JSON_PROPERTY, this.definition);
        return broadcastableObject;
    }
}
