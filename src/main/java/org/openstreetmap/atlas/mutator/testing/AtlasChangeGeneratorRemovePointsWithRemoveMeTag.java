package org.openstreetmap.atlas.mutator.testing;

import java.util.HashSet;
import java.util.Set;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEntity;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.atlas.items.ItemType;
import org.openstreetmap.atlas.geography.atlas.items.Point;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * A mutator that removes any {@link Point}s that have a "removeme" tag.
 * 
 * @author lcram
 */
public class AtlasChangeGeneratorRemovePointsWithRemoveMeTag extends ConfiguredAtlasChangeGenerator
{
    private static final long serialVersionUID = -6539776271945971284L;

    public AtlasChangeGeneratorRemovePointsWithRemoveMeTag(final String name,
            final Configuration configuration)
    {
        super(name, configuration);
    }

    @Override
    public boolean equals(final Object other)
    {
        return super.equals(other);
    }

    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        final Set<FeatureChange> result = new HashSet<>();
        for (final AtlasEntity entity : atlas.entities())
        {
            if (entity.getType() == ItemType.POINT && entity.getTags().containsKey("removeme"))
            {
                result.add(FeatureChange.remove(CompleteEntity.shallowFrom(entity)));
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }
}
