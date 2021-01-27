package org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies;

import java.io.Serializable;
import java.util.Map;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.change.ChangeType;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEdge;
import org.openstreetmap.atlas.geography.atlas.items.ItemType;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.MergeForgivenessStrategy;

/**
 * A {@link MergeForgivenessStrategy} which returns a {@link FeatureChange} which takes the lower
 * start or end node identifier when two edges conflict on that value. This protects against
 * mutations that generate nodes that already exist in the underlying atlas.
 *
 * @author jamesgage
 */
public class TakeLowerNodeIdentifierStrategy implements MergeForgivenessStrategy
{

    private static final long serialVersionUID = 5772246281692335416L;

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
        if (left.getItemType() != right.getItemType() || left.getItemType() != ItemType.EDGE)
        {
            throw new CoreException(
                    "Cannot apply merge forgiveness since FeatureChanges are not both edges: left: {} vs right: {}",
                    left.getItemType(), right.getItemType());
        }
        if (left.getChangeType() != right.getChangeType() || left.getChangeType() != ChangeType.ADD)
        {
            throw new CoreException(
                    "Cannot apply merge FeatureChanges as they are of different types: left: {} vs right: {}",
                    left.getChangeType(), right.getChangeType());
        }
        final CompleteEdge leftEdge = (CompleteEdge) left.getAfterView();
        final CompleteEdge rightEdge = (CompleteEdge) right.getAfterView();
        final CompleteEdge synthesizedEdge;

        // Either the left or the right edge will be full, base our synthesized edge off the full
        // one
        synthesizedEdge = (leftEdge.isFull()) ? leftEdge : rightEdge;

        // If a start or end node is null, take the other FeatureChange's value
        if (leftEdge.start() == null)
        {
            leftEdge.withStartNodeIdentifier(rightEdge.start().getIdentifier());
        }
        if (leftEdge.end() == null)
        {
            leftEdge.withEndNodeIdentifier(rightEdge.end().getIdentifier());
        }
        if (rightEdge.start() == null)
        {
            rightEdge.withStartNodeIdentifier(leftEdge.start().getIdentifier());
        }
        if (rightEdge.end() == null)
        {
            rightEdge.withEndNodeIdentifier(leftEdge.end().getIdentifier());
        }

        // Take the lower identifier if there is a conflict between start or end nodes
        if (leftEdge.start().getIdentifier() != rightEdge.start().getIdentifier())
        {
            synthesizedEdge.withStartNodeIdentifier(
                    Math.min(leftEdge.start().getIdentifier(), rightEdge.start().getIdentifier()));
        }
        if (leftEdge.end().getIdentifier() != rightEdge.end().getIdentifier())
        {
            synthesizedEdge.withEndNodeIdentifier(
                    Math.min(leftEdge.end().getIdentifier(), rightEdge.end().getIdentifier()));
        }
        return FeatureChange.add(synthesizedEdge);
    }

}
