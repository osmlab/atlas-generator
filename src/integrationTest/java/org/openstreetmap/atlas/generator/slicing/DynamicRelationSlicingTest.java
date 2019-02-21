package org.openstreetmap.atlas.generator.slicing;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.generator.AtlasGeneratorHelper;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.items.Line;
import org.openstreetmap.atlas.geography.atlas.raw.slicing.RawAtlasCountrySlicer;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.DynamicTileSharding;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;

/**
 * @author james-gage
 */
public class DynamicRelationSlicingTest
{
    public static final String ABC_100 = "resource://test/atlas/ABC/ABC_1-0-0.atlas";
    public static final String ABC_101 = "resource://test/atlas/ABC/ABC_1-0-1.atlas";
    public static final String ABC_110 = "resource://test/atlas/ABC/ABC_1-1-0.atlas";
    public static final String ABC_111 = "resource://test/atlas/ABC/ABC_1-1-1.atlas";
    public static final String DEF_101 = "resource://test/atlas/DEF/DEF_1-0-1.atlas";
    public static final String DEF_111 = "resource://test/atlas/DEF/DEF_1-1-1.atlas";

    static
    {
        addResource(ABC_100, "ABC_1-0-0.atlas");
        addResource(ABC_101, "ABC_1-0-1.atlas");
        addResource(ABC_110, "ABC_1-1-0.atlas");
        addResource(ABC_111, "ABC_1-1-1.atlas");
        addResource(DEF_101, "DEF_1-0-1.atlas");
        addResource(DEF_111, "DEF_1-1-1.atlas");
    }

    private static void addResource(final String path, final String name)
    {
        ResourceFileSystem.addResource(path, new InputStreamResource(
                () -> DynamicRelationSlicingTest.class.getResourceAsStream(name)));
    }

    /**
     * This integration test country slices a water relation that spans four shards, and crosses a
     * country boundary. The relation is country sliced twice, once from the ABC perspective and
     * once from DEF. The resulting closed relations then have their areas validated.
     */
    @Test
    public void testRelationSlicing()
    {
        // parameter setup
        final Sharding sharding = new DynamicTileSharding(new File(
                DynamicRelationSlicingTest.class.getResource("simpleSharding.txt").getFile()));

        final CountryBoundaryMap boundaryMap = CountryBoundaryMap
                .fromPlainText(new InputStreamResource(() -> DynamicRelationSlicingTest.class
                        .getResourceAsStream("simpleBoundaryFile.txt")));

        final Map<String, String> sparkOptions = new HashMap<>();
        sparkOptions.put("spark.executor.memory", "512m");
        sparkOptions.put("spark.executor.memory", "512m");
        sparkOptions.put("spark.driver.memory", "512m");
        ResourceFileSystem.simpleconfiguration()
                .forEach((key, value) -> sparkOptions.put(key, value));

        final HadoopAtlasFileCache lineSlicedAtlasCache = new HadoopAtlasFileCache(
                "resource://test/atlas/", sparkOptions);

        final CountryShard initialShardABC = CountryShard.forName("ABC_1-0-0");
        final CountryShard initialShardDEF = CountryShard.forName("DEF_1-1-1");

        final Function<Shard, Optional<Atlas>> atlasFetcherABC = AtlasGeneratorHelper.atlasFetcher(
                lineSlicedAtlasCache, lineSlicedAtlasCache, boundaryMap, "ABC",
                initialShardDEF.getShard());
        final Function<Shard, Optional<Atlas>> atlasFetcherDEF = AtlasGeneratorHelper.atlasFetcher(
                lineSlicedAtlasCache, lineSlicedAtlasCache, boundaryMap, "DEF",
                initialShardDEF.getShard());

        // the operation we are testing
        final Atlas relationSlicedAtlasDEF = new RawAtlasCountrySlicer("DEF", boundaryMap, sharding,
                atlasFetcherDEF).sliceRelations(initialShardDEF.getShard());
        final Atlas relationSlicedAtlasABC = new RawAtlasCountrySlicer("ABC", boundaryMap, sharding,
                atlasFetcherABC).sliceRelations(initialShardABC.getShard());

        // Sum the areas and check it is the size expected
        double totalAreaDEF = 0;
        for (final Line line : relationSlicedAtlasDEF.lines())
        {
            if (line.isClosed())
            {
                final Polygon polygon = new Polygon(line.asPolyLine());
                totalAreaDEF += polygon.surface().asDm7Squared();
            }
        }
        Assert.assertTrue(totalAreaDEF == 5.9870426059188685E17);

        double totalAreaABC = 0;
        for (final Line line : relationSlicedAtlasABC.lines())
        {
            if (line.isClosed())
            {
                final Polygon polygon = new Polygon(line.asPolyLine());
                totalAreaABC += polygon.surface().asDm7Squared();
            }
        }
        Assert.assertTrue(totalAreaABC == 8.9912430112087885E17);
    }
}
