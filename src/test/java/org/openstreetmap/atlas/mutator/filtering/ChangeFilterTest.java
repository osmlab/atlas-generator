package org.openstreetmap.atlas.mutator.filtering;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.ChangeBuilder;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEdge;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteNode;
import org.openstreetmap.atlas.geography.atlas.complete.CompletePoint;
import org.openstreetmap.atlas.geography.atlas.dynamic.DynamicAtlas;
import org.openstreetmap.atlas.geography.atlas.dynamic.policy.DynamicAtlasPolicy;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.mutator.AtlasMutator;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.Sets;

/**
 * @author matthieun
 */
public class ChangeFilterTest
{
    @Rule
    public final ChangeFilterTestRule setup = new ChangeFilterTestRule();

    @Test
    public void featureChangeWithoutMutatorTagTest()
    {
        final FeatureChange input1 = FeatureChange.add(new CompletePoint(123L, Location.COLOSSEUM,
                Maps.hashMap(AtlasMutator.MUTATOR_META_DATA_KEY
                        + AtlasMutator.MUTATOR_META_DATA_SPLIT + "MyMutation", "5"),
                null));
        final FeatureChange input2 = FeatureChange
                .add(new CompletePoint(123L, Location.COLOSSEUM, null, null));
        Assert.assertTrue(ChangeFilter.featureChangeWithoutMutatorTag(input1, featureChange -> true)
                .getAfterView().getTags().isEmpty());
        Assert.assertNull(ChangeFilter.featureChangeWithoutMutatorTag(input2, featureChange -> true)
                .getAfterView().getTags());
    }

    @Test
    public void testFilteringExtraneousNodeWithExtraneousEdgeAttached()
    {
        final Shard initialShard = SlippyTile.forName("11-1064-727");
        // Get a random atlas with data in that initial shard.
        final Atlas atlas = this.setup
                .getTestFilteringExtraneousNodeWithExtraneousEdgeAttachedAtlas();
        // Make it a DynamicAtlas for ChangeFilter
        final DynamicAtlas dynamicAtlas = new DynamicAtlas(new DynamicAtlasPolicy(
                shard -> initialShard.equals(shard) ? Optional.of(atlas) : Optional.empty(),
                Sharding.forString("slippy@11"), initialShard, initialShard.bounds()));
        // This is the expansion bounds that make sure the extraneous edge and node below are
        // properly overlapping the initial shard (like if they belonged to a large relation)
        final Rectangle boundsExtension = Rectangle
                .forLocated(Polygon.wkt("POLYGON((7.0610389 46.0838970,7.4549322 46.0838970,"
                        + "7.4549322 46.2762806,7.0610389 46.2762806,7.0610389 46.0838970))"));
        final ChangeBuilder changeBuilder = new ChangeBuilder();
        // Extraneous node that has a connected edge that is also extraneous
        final FeatureChange nodeFeatureChange = FeatureChange
                .add(new CompleteNode(123L, Location.forWkt("POINT(7.2415974 46.2402308)"),
                        Maps.hashMap(), Sets.treeSet(456L), Sets.treeSet(), Sets.hashSet())
                                .withBoundsExtendedBy(boundsExtension));
        // Add meta data which indicates the node comes from another shard than the initial one
        nodeFeatureChange.addMetaData(AtlasMutator.MUTATOR_META_DATA_KEY,
                "myRandomMutator" + AtlasMutator.MUTATOR_META_DATA_SPLIT + "11-1065-726");
        // Add the remove feature change for the connected extraneous edge
        final FeatureChange edgeFeatureChange = FeatureChange
                .remove(new CompleteEdge(456L, null, null, null, null, null)
                        .withBoundsExtendedBy(boundsExtension));
        changeBuilder.add(nodeFeatureChange);
        changeBuilder.add(edgeFeatureChange);
        final Change change = changeBuilder.get();
        final ChangeFilter changeFilter = new ChangeFilter(dynamicAtlas);
        final Optional<Change> newChangeOption = changeFilter.apply(change);
        Assert.assertTrue(newChangeOption.isEmpty());
    }
}
