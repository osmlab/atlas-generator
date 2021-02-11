package org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies;

import java.io.Serializable;
import java.util.Map;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.change.ChangeType;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.MergeForgivenessStrategy;

/**
 * This {@link MergeForgivenessStrategy} always tries to choose a {@link ChangeType#REMOVE}
 * {@link FeatureChange} over a {@link ChangeType#ADD} * {@link FeatureChange}.
 *
 * @author lcram
 */
public class PickRemoveOverAddStrategy implements MergeForgivenessStrategy
{
    private static final long serialVersionUID = 64863373477818800L;

    @Override
    public FeatureChange resolve(final FeatureChange left, final FeatureChange right,
            final Map<String, Serializable> configuration)
    {
        if (left.getIdentifier() != right.getIdentifier())
        {
            throw new CoreException(
                    "Cannot choose a FeatureChange since identifiers conflict: left: {} vs right: {}",
                    left.getIdentifier(), right.getIdentifier());
        }
        if (left.getItemType() != right.getItemType())
        {
            throw new CoreException(
                    "Cannot choose a FeatureChange since ItemTypes conflict: left: {} vs right: {}",
                    left.getItemType(), right.getItemType());
        }
        if (left.getChangeType() == ChangeType.REMOVE)
        {
            return left;

        }
        else if (right.getChangeType() == ChangeType.REMOVE)
        {
            return right;
        }
        else
        {
            throw new CoreException(
                    "Cannot choose a FeatureChange since neither is of type REMOVE");
        }
    }
}
