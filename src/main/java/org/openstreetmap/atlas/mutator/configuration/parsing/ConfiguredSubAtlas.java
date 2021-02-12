package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Function;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;
import org.openstreetmap.atlas.utilities.configuration.ConfiguredFilter;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

/**
 * @author matthieun
 */
public final class ConfiguredSubAtlas implements Function<Atlas, Optional<Atlas>>, Serializable
{
    public static final String DEFAULT = "default";
    public static final ConfiguredSubAtlas UNCHANGED = new ConfiguredSubAtlas();

    public static final String TYPE_JSON_PROPERTY_VALUE = "_subAtlas";
    public static final String NAME_JSON_PROPERTY = "name";
    public static final String FILTER_JSON_PROPERTY = "filter";
    public static final String CUT_TYPE_JSON_PROPERTY = "cutType";

    private static final long serialVersionUID = -2506410811249068520L;
    private static final Logger logger = LoggerFactory.getLogger(ConfiguredSubAtlas.class);
    private static final String CONFIGURATION_ROOT = AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL
            + ".subAtlases";
    private static final String CONFIGURATION_FILTER = "filter";
    private static final String CONFIGURATION_CUT_TYPE = "cutType";

    private final ConfiguredFilter subAtlasFilter;
    private final AtlasCutType subAtlasCutType;
    private final String name;

    public static ConfiguredSubAtlas from(final String name, final Configuration configuration)
    {
        if (DEFAULT.equals(name))
        {
            return UNCHANGED;
        }
        if (!new ConfigurationReader(CONFIGURATION_ROOT).isPresent(configuration, name))
        {
            logger.warn(
                    "Attempted to create ConfiguredSubAtlas called \"{}\" but it was not found. It will be swapped with default passthrough sub atlas.",
                    name);
            return UNCHANGED;
        }
        return new ConfiguredSubAtlas(name, configuration);
    }

    private ConfiguredSubAtlas()
    {
        this(DEFAULT, new StandardConfiguration(new StringResource("{}")));
    }

    private ConfiguredSubAtlas(final String name, final Configuration configuration)
    {
        final ConfigurationReader reader = new ConfigurationReader(CONFIGURATION_ROOT + "." + name);
        this.name = name;
        this.subAtlasFilter = ConfiguredFilter.from(reader.configurationValue(configuration,
                CONFIGURATION_FILTER, ConfiguredFilter.DEFAULT), configuration);
        this.subAtlasCutType = AtlasCutType.valueOf(reader.configurationValue(configuration,
                CONFIGURATION_CUT_TYPE, AtlasCutType.SOFT_CUT.name()));
    }

    @Override
    public Optional<Atlas> apply(final Atlas atlas)
    {
        if (atlas == null)
        {
            return Optional.empty();
        }
        if (this.subAtlasFilter == ConfiguredFilter.NO_FILTER)
        {
            return Optional.of(atlas);
        }
        return atlas.subAtlas(this.subAtlasFilter, this.subAtlasCutType);
    }

    public String getName()
    {
        return this.name;
    }

    public AtlasCutType getSubAtlasCutType()
    {
        return this.subAtlasCutType;
    }

    public ConfiguredFilter getSubAtlasFilter()
    {
        return this.subAtlasFilter;
    }

    public JsonObject toJson()
    {
        final JsonObject subAtlasObject = new JsonObject();
        subAtlasObject.addProperty(AtlasMutatorConfiguration.TYPE_JSON_PROPERTY,
                TYPE_JSON_PROPERTY_VALUE);
        subAtlasObject.addProperty(NAME_JSON_PROPERTY, this.name);
        subAtlasObject.addProperty(FILTER_JSON_PROPERTY, this.subAtlasFilter.getName()); // NOSONAR
        subAtlasObject.addProperty(CUT_TYPE_JSON_PROPERTY, this.subAtlasCutType.name()); // NOSONAR
        return subAtlasObject;
    }
}
