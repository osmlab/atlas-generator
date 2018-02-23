package org.openstreetmap.atlas.generator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.boundary.CountryBoundary;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.DynamicTileSharding;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;

/**
 * @author matthieun
 */
public class AtlasGeneratorTest
{
    @Test
    public void testRoughFilterShards()
    {
        final CountryBoundaryMap countryBoundaryMapHTI = new CountryBoundaryMap(
                new InputStreamResource(
                        () -> AtlasGeneratorTest.class.getResourceAsStream("HTI.txt")));
        final CountryBoundaryMap countryBoundaryMapJAM = new CountryBoundaryMap(
                new InputStreamResource(
                        () -> AtlasGeneratorTest.class.getResourceAsStream("JAM.txt")));

        final List<CountryBoundary> countryBoundaryHTI = countryBoundaryMapHTI
                .countryBoundary("HTI");
        final List<CountryBoundary> countryBoundaryJAM = countryBoundaryMapJAM
                .countryBoundary("JAM");

        final Sharding sharding = new DynamicTileSharding(new InputStreamResource(
                () -> AtlasGenerator.class.getResourceAsStream("tree-6-14-100000.txt")));

        final Set<Shard> shardsHTI = AtlasGenerator.roughShards(sharding,
                countryBoundaryHTI.get(0));
        final Set<Shard> filteredOutHTI = shardsHTI.stream()
                .filter(shard -> !AtlasGenerator.filterShards(shard, countryBoundaryHTI))
                .collect(Collectors.toSet());
        Assert.assertEquals(2, filteredOutHTI.size());
        Assert.assertTrue(filteredOutHTI.contains(new SlippyTile(74, 115, 8)));
        Assert.assertTrue(filteredOutHTI.contains(new SlippyTile(77, 115, 8)));

        final Set<Shard> shardsJAM = AtlasGenerator.roughShards(sharding,
                countryBoundaryJAM.get(0));
        final Set<Shard> filteredOutJAM = shardsJAM.stream()
                .filter(shard -> !AtlasGenerator.filterShards(shard, countryBoundaryJAM))
                .collect(Collectors.toSet());
        Assert.assertEquals(1, filteredOutJAM.size());
        Assert.assertTrue(filteredOutJAM.contains(new SlippyTile(74, 114, 8)));
    }
}
