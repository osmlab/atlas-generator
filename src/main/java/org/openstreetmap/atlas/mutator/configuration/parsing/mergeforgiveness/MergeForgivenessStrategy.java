package org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness;

import java.io.Serializable;
import java.util.Map;

import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;

/**
 * The {@link MergeForgivenessStrategy} interface defines a strategy for resolving
 * {@link FeatureChange} merge conflicts. Implementing classes must provide a method that decides
 * which {@link FeatureChange} to choose in the face of a merge conflict.
 * 
 * @author lcram
 */
public interface MergeForgivenessStrategy extends Serializable
{
    /**
     * Resolve a merge conflict between two {@link FeatureChange}s and return a new, valid
     * {@link FeatureChange} that represents a merge between the two.
     * 
     * @param left
     *            the left {@link FeatureChange}
     * @param right
     *            the right {@link FeatureChange}
     * @param configuration
     *            the configuration for the resolution
     * @return the resolved {@link FeatureChange}
     */
    FeatureChange resolve(FeatureChange left, FeatureChange right,
            Map<String, Serializable> configuration);
}
