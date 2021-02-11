package org.openstreetmap.atlas.mutator.testing;

import java.util.Map;

import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.mutator.configuration.broadcast.ConfiguredBroadcastable;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * Example of a broadcastable sharding object
 * 
 * @author matthieun
 */
public class ConfiguredBroadcastableAtlasSharding extends ConfiguredBroadcastable
{
    private static final long serialVersionUID = 4200315866327152006L;

    public ConfiguredBroadcastableAtlasSharding(final String name,
            final Configuration configuration)
    {
        super(name, configuration);
    }

    @Override
    public Sharding read(final String definition, final Map<String, String> configuration)
    {
        return AtlasSharding.forString(definition, configuration);
    }
}
