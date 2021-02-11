package org.openstreetmap.atlas.mutator.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * @author matthieun
 */
public class AtlasChangeGeneratorAddShard extends AtlasChangeGeneratorAddTag
{
    private static final long serialVersionUID = 782434242891149771L;
    private List<Sharding> shardings = null;

    public AtlasChangeGeneratorAddShard(final String name, final Configuration configuration)
    {
        super(name, configuration);
    }

    @Override
    public boolean equals(final Object other)
    {
        return super.equals(other);
    }

    @Override
    public String getValue(final AtlasEntity entity)
    {
        setup();
        final StringList result = new StringList();
        for (final Sharding sharding : this.shardings)
        {
            result.add(
                    sharding.shardsCovering(entity.bounds().center()).iterator().next().getName());
        }
        return result.join(",");
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    private void setup()
    {
        if (this.shardings == null)
        {
            this.shardings = new ArrayList<>();
            for (final String broadcastName : this.getBroadcastVariablesNeeded().keySet())
            {
                final Object broadcasted = this.getBroadcastVariable(broadcastName);
                if (broadcasted instanceof Sharding)
                {
                    this.shardings.add((Sharding) broadcasted);
                }
                else if (broadcasted instanceof Map)
                {
                    this.shardings.addAll(((Map<String, Sharding>) broadcasted).values());
                }
                else
                {
                    throw new CoreException("Cannot recognize broadcast variable of type {}",
                            broadcasted.getClass().getSimpleName());
                }
            }
        }
    }
}
