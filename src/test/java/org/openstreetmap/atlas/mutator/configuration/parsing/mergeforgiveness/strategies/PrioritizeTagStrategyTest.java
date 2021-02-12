package org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompletePoint;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlasBuilder;
import org.openstreetmap.atlas.tags.SyntheticDuplicateOsmNodeTag;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lcram
 */
public class PrioritizeTagStrategyTest
{
    private static final Logger logger = LoggerFactory.getLogger(PrioritizeTagStrategyTest.class);

    public Atlas getAtlasWithSyntheticTag()
    {
        final PackedAtlasBuilder builder = new PackedAtlasBuilder();
        builder.addPoint(1L, Location.CENTER, Maps.hashMap("foo", "bar",
                SyntheticDuplicateOsmNodeTag.KEY, SyntheticDuplicateOsmNodeTag.YES.toString()));
        return builder.get();
    }

    public Atlas getAtlasWithoutSyntheticTag()
    {
        final PackedAtlasBuilder builder = new PackedAtlasBuilder();
        builder.addPoint(1L, Location.CENTER, Maps.hashMap("foo", "bar"));
        return builder.get();
    }

    @Test
    public void testMergeResolutionLeftHasSyntheticDuplicateOsmNodeTag()
    {
        final PrioritizeTagStrategy strategy = new PrioritizeTagStrategy();
        final CompletePoint leftPointAfterView = new CompletePoint(1L, Location.COLOSSEUM,
                Maps.hashMap("foo", "bar", SyntheticDuplicateOsmNodeTag.KEY,
                        SyntheticDuplicateOsmNodeTag.YES.toString(), "baz", "bat"),
                null);
        final CompletePoint rightPointAfterView = new CompletePoint(1L, Location.COLOSSEUM,
                Maps.hashMap("foo", "bar", "baz", "bat"), null);

        final FeatureChange left = FeatureChange.add(leftPointAfterView,
                getAtlasWithSyntheticTag());
        final FeatureChange right = FeatureChange.add(rightPointAfterView,
                getAtlasWithoutSyntheticTag());
        final Map<String, Serializable> configuration = new HashMap<>();
        final List<Map<String, Serializable>> rules = new ArrayList<>();
        final Map<String, Serializable> rule1 = new HashMap<>();
        rule1.put(PrioritizeTagStrategy.PrioritizationRule.NAME_CONFIG, "synthetic_duplicate_osm");
        rule1.put(PrioritizeTagStrategy.PrioritizationRule.FILTER_CONFIG,
                SyntheticDuplicateOsmNodeTag.KEY + "->" + SyntheticDuplicateOsmNodeTag.YES);
        rules.add(rule1);
        configuration.put(PrioritizeTagStrategy.RULES, (Serializable) rules);

        final FeatureChange result = strategy.resolve(left, right, configuration);
        final CompletePoint resultingPoint = (CompletePoint) result.getAfterView();
        Assert.assertEquals(SyntheticDuplicateOsmNodeTag.YES.toString(),
                resultingPoint.tag(SyntheticDuplicateOsmNodeTag.KEY));
    }
}
