package org.openstreetmap.atlas.mutator.configuration;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.DynamicTileSharding;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.mutator.AtlasMutator;
import org.openstreetmap.atlas.mutator.testing.ConfiguredAtlasChangeGeneratorAddTurnRestrictions;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;

/**
 * @author matthieun
 */
public class AtlasMutationLevelTest
{
    @Test
    public void testGetCountryShards()
    {
        final AtlasMutationLevel level = getLevel(Sets.hashSet("XYZ", "JKL", "ABC"), "GROUP0");
        Assert.assertEquals(12, Iterables.size(level.shards()));
    }

    @Test
    public void testMerge()
    {
        final AtlasMutationLevel level0 = getLevel(Sets.hashSet("ABC"), "ABC");
        level0.setAllowRDD(true);
        final AtlasMutationLevel level1 = getLevel(Sets.hashSet("ABC"), "ABC");
        level1.notifyOfChildLevel(level0);
        level1.notifyOfParentLevel(level0);
        final AtlasMutationLevel level2 = getLevel(Sets.hashSet("XYZ"), "XYZ");
        level2.notifyOfChildLevel(level0);
        level2.notifyOfParentLevel(level0);
        final AtlasMutationLevel level = level1.merge(level2, "GROUP0");
        Assert.assertTrue(level.isChildNeedsRDDInput());
        Assert.assertTrue(level.isParentNeedsRDDInput());
    }

    private AtlasMutationLevel getLevel(final Set<String> countries, final String name)
    {
        final CountryBoundaryMap boundaries = new CountryBoundaryMap();
        boundaries.readFromPlainText(new InputStreamResource(() -> AtlasMutationLevelTest.class
                .getResourceAsStream("AtlasMutationLevelTestBoundaries.txt")));
        final Sharding sharding = new DynamicTileSharding(new InputStreamResource(
                () -> AtlasMutator.class.getResourceAsStream("tree-6-14-100000.txt")));
        final Configuration configuration = new StandardConfiguration(
                new InputStreamResource(() -> AtlasMutationLevelTest.class
                        .getResourceAsStream("testAtlasMutationLevel.json")));
        final AtlasMutatorConfiguration atlasMutatorConfiguration = new AtlasMutatorConfiguration(
                countries, sharding, boundaries, "", "", Maps.hashMap(), configuration, true, true,
                true);
        final AtlasMutationLevel result = new AtlasMutationLevel(atlasMutatorConfiguration, name,
                countries, Sets.hashSet(new ConfiguredAtlasChangeGeneratorAddTurnRestrictions(
                        "AtlasChangeGeneratorAddTurnRestrictions", configuration)),
                1, 1);
        result.setAllowRDD(true);
        result.setPreloadRDD(true);
        return result;
    }
}
