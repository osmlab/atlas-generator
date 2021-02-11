package org.openstreetmap.atlas.mutator.testing;

import java.util.HashSet;
import java.util.Set;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEntity;
import org.openstreetmap.atlas.geography.atlas.complete.CompletePoint;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.atlas.items.ItemType;
import org.openstreetmap.atlas.geography.atlas.items.Point;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * A test mutation that generates {@link FeatureChange}s which conflict with each other (it tries to
 * add a "foo=bar" tag to all {@link Point}s, but also tries to remove all {@link Point}s).
 * 
 * @author lcram
 */
public class AtlasChangeGeneratorSelfConflicting extends ConfiguredAtlasChangeGenerator
{
    private static final long serialVersionUID = 6116168840419629735L;

    public AtlasChangeGeneratorSelfConflicting(final String name, final Configuration configuration)
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
            if (entity.getType() == ItemType.POINT)
            {
                final CompletePoint point = CompletePoint.shallowFrom((Point) entity);
                point.withTags(entity.getTags()).withAddedTag("foo", "bar");
                result.add(FeatureChange.add(point));
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
