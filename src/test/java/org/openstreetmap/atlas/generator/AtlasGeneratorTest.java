package org.openstreetmap.atlas.generator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.DynamicTileSharding;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.maps.MultiMap;

/**
 * Tests for {@link AtlasGenerator}.
 *
 * @author matthieun
 * @author mkalender
 */
public class AtlasGeneratorTest
{
    private static final Sharding SHARDING = new DynamicTileSharding(new InputStreamResource(
            () -> AtlasGenerator.class.getResourceAsStream("tree-6-14-100000.txt")));
    private static final CountryBoundaryMap HTI_BOUNDARY_MAP = CountryBoundaryMap.fromPlainText(
            new InputStreamResource(() -> AtlasGeneratorTest.class.getResourceAsStream("HTI.txt")));
    private static final CountryBoundaryMap JAM_BOUNDARY_MAP = CountryBoundaryMap.fromPlainText(
            new InputStreamResource(() -> AtlasGeneratorTest.class.getResourceAsStream("JAM.txt")));

    @Test
    public void testGenerateTasksEmptyBoundary()
    {
        final CountryBoundaryMap boundaryMap = new CountryBoundaryMap();
        Assert.assertTrue(AtlasGenerator.generateTasks(new StringList("HTI"), boundaryMap, SHARDING)
                .isEmpty());
    }

    @Test
    public void testGenerateTasksEmptyBoundaryAndCountryList()
    {
        final CountryBoundaryMap boundaryMap = new CountryBoundaryMap();
        Assert.assertTrue(
                AtlasGenerator.generateTasks(new StringList(), boundaryMap, SHARDING).isEmpty());
    }

    @Test
    public void testGenerateTasksEmptyCountryList()
    {
        Assert.assertTrue(AtlasGenerator.generateTasks(new StringList(), HTI_BOUNDARY_MAP, SHARDING)
                .isEmpty());
    }

    @Test
    public void testGenerateTasksHTI()
    {
        // HTI boundary should have generated 36 tasks
        testCountry(new StringList("HTI"), HTI_BOUNDARY_MAP, 36);
    }

    @Test
    public void testGenerateTasksJAM()
    {
        // JAM boundary should have generated 6 tasks
        testCountry(new StringList("JAM"), JAM_BOUNDARY_MAP, 6);
    }

    @Test
    public void testGenerateTasksWrongCountryList()
    {
        Assert.assertTrue(AtlasGenerator
                .generateTasks(new StringList("ABC", "XYZ"), JAM_BOUNDARY_MAP, SHARDING).isEmpty());
    }

    private void testCountry(final StringList countries, final CountryBoundaryMap boundaryMap,
            final int expectedTaskSize)
    {
        final MultiMap<String, AtlasGenerationTask> tasks = AtlasGenerator.generateTasks(countries,
                boundaryMap, SHARDING);
        final List<AtlasGenerationTask> allTasks = tasks.allValues();
        Assert.assertEquals(expectedTaskSize, allTasks.size());

        // Verify that tasks have the same set of shards for the country
        verifyAllShardEquality(allTasks);

        // Verify no shard is missed from tasks
        verifyNoShardIsMissing(allTasks);
    }

    private void verifyAllShardEquality(final List<AtlasGenerationTask> tasks)
    {
        final String referenceCountry = tasks.get(0).getCountry();
        final Set<Shard> referenceShards = tasks.get(0).getAllShards();
        tasks.stream().skip(1).forEach(otherTask ->
        {
            Assert.assertEquals(referenceCountry, otherTask.getCountry());
            Assert.assertEquals(referenceShards, otherTask.getAllShards());
        });
    }

    private void verifyNoShardIsMissing(final List<AtlasGenerationTask> tasks)
    {
        final Set<Shard> referenceShards = tasks.get(0).getAllShards();
        final Set<Shard> taskShards = tasks.stream().map(task -> task.getShard())
                .collect(Collectors.toSet());
        Assert.assertEquals(referenceShards, taskShards);
    }
}
