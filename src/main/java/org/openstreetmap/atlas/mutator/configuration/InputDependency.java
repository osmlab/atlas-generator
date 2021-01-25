package org.openstreetmap.atlas.mutator.configuration;

import java.io.Serializable;
import java.util.Objects;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.parsing.AtlasMutatorConfigurationParser;
import org.openstreetmap.atlas.mutator.configuration.parsing.ConfiguredSubAtlas;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;

import com.google.gson.JsonObject;

/**
 * Defines an extra Atlas {@link Shard} dependency that a level requires from the previous mutated
 * level.
 *
 * @author matthieun
 */
public class InputDependency implements Serializable
{
    public static final String INPUT_DEPENDENCY_FOLDER_KEY = "_inputDependency_";
    public static final String CONFIGURATION_ROOT = AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL
            + ".inputDependencies";

    public static final String TYPE_JSON_PROPERTY_VALUE = "_inputDependency";
    public static final String PATH_NAME_JSON_PROPERTY = "pathName";
    public static final String OWNER_JSON_PROPERTY = "owner";
    public static final String SUB_ATLAS_JSON_PROPERTY = "subAtlas";

    private static final long serialVersionUID = 1580149119801444739L;
    private final ConfiguredAtlasChangeGenerator owner;
    private final String pathName;
    private final ConfiguredSubAtlas subAtlas;

    public InputDependency(final ConfiguredAtlasChangeGenerator owner, final String pathName,
            final Configuration configuration)
    {
        this.owner = owner;
        this.pathName = pathName;
        final ConfigurationReader reader = new ConfigurationReader(CONFIGURATION_ROOT);
        if (!reader.isPresent(configuration, pathName))
        {
            throw new CoreException(
                    "{} defines an input dependency {} that is not found in the global definitions.",
                    owner, pathName);
        }
        final String subAtlasName = reader.configurationValue(configuration, pathName,
                ConfiguredSubAtlas.DEFAULT);
        this.subAtlas = ConfiguredSubAtlas.from(subAtlasName, configuration);
    }

    @Override
    public boolean equals(final Object other)
    {
        if (other instanceof InputDependency)
        {
            return this.getPathName().equals(((InputDependency) other).getPathName());
        }
        return false;
    }

    public ConfiguredAtlasChangeGenerator getOwner()
    {
        return this.owner;
    }

    public String getPathName()
    {
        return this.pathName;
    }

    public ConfiguredSubAtlas getSubAtlas()
    {
        return this.subAtlas;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getPathName());
    }

    public JsonObject toJson()
    {
        final JsonObject dependencyObject = new JsonObject();
        dependencyObject.addProperty(AtlasMutatorConfiguration.TYPE_JSON_PROPERTY,
                TYPE_JSON_PROPERTY_VALUE);
        if (this.pathName != null && !this.pathName.isEmpty())
        {
            dependencyObject.addProperty(PATH_NAME_JSON_PROPERTY, this.pathName);
        }
        if (this.owner != null)
        {
            dependencyObject.addProperty(OWNER_JSON_PROPERTY, this.owner.getName());
        }
        if (this.subAtlas != null)
        {
            dependencyObject.addProperty(SUB_ATLAS_JSON_PROPERTY, this.subAtlas.getName());
        }
        return dependencyObject;
    }

    @Override
    public String toString()
    {
        return "InputDependency_" + this.pathName;
    }
}
