package org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.PolyLine;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEdge;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteNode;

/**
 * Tests the functionality of the {@link TakeLowerNodeIdentifierStrategy}.
 *
 * @author jamesgage
 */
public class TakeLowerNodeIdentifierStrategyTest
{
    @Test
    public void testMergeResolution()
    {
        final TakeLowerNodeIdentifierStrategy strategy = new TakeLowerNodeIdentifierStrategy();
        final HashMap<String, String> tags = new HashMap<>();
        tags.put("James", "Cool");
        final HashSet<Long> relationIdentifiers = new HashSet<>();
        relationIdentifiers.add(10101L);
        final CompleteEdge leftEdge = new CompleteEdge(100L, PolyLine.TEST_POLYLINE, tags, 50L, 51L,
                relationIdentifiers);
        final CompleteEdge rightEdge = new CompleteEdge(100L, PolyLine.TEST_POLYLINE, null, null,
                5L, null);
        final FeatureChange left = FeatureChange.add(leftEdge);
        final FeatureChange right = FeatureChange.add(rightEdge);
        final Map<String, Serializable> configuration = new HashMap<>();
        final FeatureChange result = strategy.resolve(left, right, configuration);
        final CompleteEdge resultingEdge = (CompleteEdge) result.getAfterView();
        Assert.assertEquals(100L, resultingEdge.getIdentifier());
        Assert.assertEquals(50L, resultingEdge.start().getIdentifier());
        Assert.assertEquals(5L, resultingEdge.end().getIdentifier());
        Assert.assertEquals("Cool", resultingEdge.tag("James"));
    }

    @Test
    public void testSkipConditions()
    {
        final TakeLowerNodeIdentifierStrategy strategy = new TakeLowerNodeIdentifierStrategy();
        final CompleteEdge rightEdge = new CompleteEdge(100L, PolyLine.TEST_POLYLINE, null, null,
                5L, null);
        final CompleteNode leftNode = new CompleteNode(100L, Location.TEST_1, null, null, null,
                null);
        FeatureChange left = FeatureChange.add(leftNode);
        final FeatureChange right = FeatureChange.add(rightEdge);
        final Map<String, Serializable> configuration = new HashMap<>();
        try
        {
            final FeatureChange result = strategy.resolve(left, right, configuration);
        }
        catch (final Exception e)
        {
            Assert.assertEquals(
                    "Cannot apply merge forgiveness since FeatureChanges are not both edges: left: NODE vs right: EDGE",
                    e.getMessage());
        }
        left = FeatureChange.add(leftNode.withIdentifier(10L));
        try
        {
            final FeatureChange result = strategy.resolve(left, right, configuration);
        }
        catch (final Exception e)
        {
            Assert.assertEquals(
                    "Cannot choose a FeatureChange since identifiers conflict: left: 10 vs right: 100",
                    e.getMessage());
        }
        left = FeatureChange.remove(rightEdge);
        try
        {
            final FeatureChange result = strategy.resolve(left, right, configuration);
        }
        catch (final Exception e)
        {
            Assert.assertEquals(
                    "Cannot apply merge FeatureChanges as they are of different types: left: REMOVE vs right: ADD",
                    e.getMessage());
        }
    }
}
