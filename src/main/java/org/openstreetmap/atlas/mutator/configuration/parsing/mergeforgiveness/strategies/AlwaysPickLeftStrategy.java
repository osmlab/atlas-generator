package org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies;

import java.io.Serializable;
import java.util.Map;

import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.MergeForgivenessStrategy;

/**
 * This {@link MergeForgivenessStrategy} always chooses the left {@link FeatureChange}.
 * 
 * @author lcram
 */
public class AlwaysPickLeftStrategy implements MergeForgivenessStrategy
{
    private static final long serialVersionUID = -3574767173621709375L;

    @Override
    public FeatureChange resolve(final FeatureChange left, final FeatureChange right,
            final Map<String, Serializable> configuration)
    {
        return left;
    }
}
