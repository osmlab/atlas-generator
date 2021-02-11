package org.openstreetmap.atlas.mutator.configuration;

import java.util.HashSet;
import java.util.Set;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompletePoint;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;

/**
 * This is a simple {@link AtlasChangeGenerator} which is not a
 * {@link ConfiguredAtlasChangeGenerator} so it cannot be configured but can still be instantiated
 * 
 * @author matthieun
 */
public class AtlasChangeGeneratorNotConfigured implements AtlasChangeGenerator
{
    private static final long serialVersionUID = 580258196933768687L;

    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        final Set<FeatureChange> result = new HashSet<>();
        result.add(FeatureChange.remove(new CompletePoint(1L, null, null, null)));
        return result;
    }
}
