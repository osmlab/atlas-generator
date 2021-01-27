package org.openstreetmap.atlas.mutator.testing;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.mutator.configuration.broadcast.ConfiguredBroadcastable;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * Group of sharding objects that are broadcastable under a map name to sharding
 * 
 * @author matthieun
 */
public class ConfiguredBroadcastableAtlasShardingGroup extends ConfiguredBroadcastable
{
    private static final long serialVersionUID = 4200315866327152006L;

    public ConfiguredBroadcastableAtlasShardingGroup(final String name,
            final Configuration configuration)
    {
        super(name, configuration);
    }

    @Override
    public Map<String, Sharding> read(final String definition,
            final Map<String, String> configuration)
    {
        final StringList split = StringList.split(definition, ",");
        final Map<String, Sharding> result = new HashMap<>();
        for (final String name : split)
        {
            result.put(name, AtlasSharding.forString(name, configuration));
        }
        return result;
    }
}
