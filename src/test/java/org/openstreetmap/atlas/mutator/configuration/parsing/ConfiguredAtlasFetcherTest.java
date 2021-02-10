package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.util.Optional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.mutator.configuration.InputDependency;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.maps.MultiMapWithSet;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author matthieun
 */
public class ConfiguredAtlasFetcherTest
{
    /**
     * @author matthieun
     */
    public static class ConfiguredAtlasFetcherTestRule extends CoreTestRule
    {
        @TestAtlas(loadFromJosmOsmResource = "ConfiguredAtlasFetcherTest.josm.osm")
        private Atlas atlas;

        @TestAtlas(loadFromJosmOsmResource = "ConfiguredAtlasFetcherTest_Country2.josm.osm")
        private Atlas atlas2;

        public Atlas getAtlas()
        {
            return this.atlas;
        }

        public Atlas getAtlas2()
        {
            return this.atlas2;
        }
    }

    public static final String INPUT = "resource://test/input";
    public static final String COUNTRY = "XYZ";
    public static final String COUNTRY_2 = "ABC";
    public static final String TILE1 = "1-1-1";
    public static final String TILE2 = "2-1-1";

    @Rule
    public final ConfiguredAtlasFetcherTestRule rule = new ConfiguredAtlasFetcherTestRule();

    private Configuration configuration;

    @After
    public void destroy()
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
    }

    @Before
    public void prepare()
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
        this.configuration = new StandardConfiguration(
                new InputStreamResource(() -> ConfiguredAtlasFetcherTest.class.getResourceAsStream(
                        ConfiguredAtlasFetcherTest.class.getSimpleName() + ".json")));
    }

    @Ignore("This test is too slow with the quadratic retry in NamespaceCachingStrategy")
    @Test(expected = RuntimeException.class)
    public void testBrokenAtlasResource()
    {
        final Resource atlasResource = new StringResource("Something not an Atlas.");
        ResourceFileSystem.addResource(INPUT + InputDependency.INPUT_DEPENDENCY_FOLDER_KEY
                + "junctionRoundaboutInputDependency/" + COUNTRY + "/" + COUNTRY + "_" + TILE1
                + FileSuffix.ATLAS, atlasResource);
        ResourceFileSystem.addResource(
                INPUT + "/" + COUNTRY + "/" + COUNTRY + "_" + TILE2 + FileSuffix.ATLAS,
                atlasResource);

        final ConfiguredAtlasFetcher junctionRoundaboutFetcher = ConfiguredAtlasFetcher
                .from("junctionRoundaboutFetcher", this.configuration);
        junctionRoundaboutFetcher
                .getFetcher(INPUT, COUNTRY, ResourceFileSystem.simpleconfiguration())
                .apply(SlippyTile.forName(TILE1));
    }

    @Test
    public void testCountryvoreEmpty()
    {
        saveAtlas(this.rule.getAtlas(),
                INPUT + "/" + COUNTRY + "/" + COUNTRY + "_" + TILE1 + FileSuffix.ATLAS);
        saveAtlas(this.rule.getAtlas2(),
                INPUT + "/" + COUNTRY_2 + "/" + COUNTRY_2 + "_" + TILE1 + FileSuffix.ATLAS);

        final ConfiguredAtlasFetcher countryvoreFetcher = ConfiguredAtlasFetcher
                .from("countryvoreFetcher", this.configuration);

        // This makes sure that if the requested shard (Tile2) is missing in te shardToCountries
        // map, it will not throw a NPE
        final MultiMapWithSet<Shard, String> shardToCountries = new MultiMapWithSet<Shard, String>();
        shardToCountries.add(SlippyTile.forName(TILE1), COUNTRY);
        shardToCountries.add(SlippyTile.forName(TILE1), COUNTRY_2);
        final Optional<Atlas> countryvoreEmptyAtlas = countryvoreFetcher
                .withShardsToCountries(shardToCountries)
                .getFetcher(INPUT, COUNTRY, ResourceFileSystem.simpleconfiguration())
                .apply(SlippyTile.forName(TILE2));
        Assert.assertTrue(countryvoreEmptyAtlas.isEmpty());

        // This makes sure that if Atlas is not present for the requested shard, we get no Atlas.
        shardToCountries.add(SlippyTile.forName(TILE2), COUNTRY);
        shardToCountries.add(SlippyTile.forName(TILE2), COUNTRY_2);
        final Optional<Atlas> countryvoreEmptyAtlas2 = countryvoreFetcher
                .withShardsToCountries(shardToCountries)
                .getFetcher(INPUT, COUNTRY, ResourceFileSystem.simpleconfiguration())
                .apply(SlippyTile.forName(TILE2));
        Assert.assertTrue(countryvoreEmptyAtlas2.isEmpty());

    }

    @Test
    public void testCountryvoreSameShard()
    {
        saveAtlas(this.rule.getAtlas(),
                INPUT + "/" + COUNTRY + "/" + COUNTRY + "_" + TILE1 + FileSuffix.ATLAS);
        saveAtlas(this.rule.getAtlas2(),
                INPUT + "/" + COUNTRY_2 + "/" + COUNTRY_2 + "_" + TILE1 + FileSuffix.ATLAS);

        final ConfiguredAtlasFetcher countryvoreFetcher = ConfiguredAtlasFetcher
                .from("countryvoreFetcher", this.configuration);
        final MultiMapWithSet<Shard, String> shardToCountries = new MultiMapWithSet<Shard, String>();
        shardToCountries.add(SlippyTile.forName(TILE1), COUNTRY);
        shardToCountries.add(SlippyTile.forName(TILE1), COUNTRY_2);
        final Atlas countryvoreSameShardAtlas = countryvoreFetcher
                .withShardsToCountries(shardToCountries)
                .getFetcher(INPUT, COUNTRY, ResourceFileSystem.simpleconfiguration())
                .apply(SlippyTile.forName(TILE1))
                .orElseThrow(() -> new CoreException("{} was not there!", TILE1));
        Assert.assertEquals(5, countryvoreSameShardAtlas.numberOfEdges());
    }

    @Test
    public void testCountryvoreSeparateShards()
    {
        saveAtlas(this.rule.getAtlas(),
                INPUT + "/" + COUNTRY + "/" + COUNTRY + "_" + TILE1 + FileSuffix.ATLAS);
        saveAtlas(this.rule.getAtlas2(),
                INPUT + "/" + COUNTRY_2 + "/" + COUNTRY_2 + "_" + TILE2 + FileSuffix.ATLAS);

        final ConfiguredAtlasFetcher countryvoreFetcher = ConfiguredAtlasFetcher
                .from("countryvoreFetcher", this.configuration);
        final MultiMapWithSet<Shard, String> shardToCountries = new MultiMapWithSet<Shard, String>();
        shardToCountries.add(SlippyTile.forName(TILE1), COUNTRY);
        shardToCountries.add(SlippyTile.forName(TILE2), COUNTRY_2);
        final Atlas countryvoreSeparateShardsAtlas = countryvoreFetcher
                .withShardsToCountries(shardToCountries)
                .getFetcher(INPUT, COUNTRY, ResourceFileSystem.simpleconfiguration())
                .apply(SlippyTile.forName(TILE1))
                .orElseThrow(() -> new CoreException("{} was not there!", TILE1));
        Assert.assertEquals(4, countryvoreSeparateShardsAtlas.numberOfEdges());
    }

    @Test
    public void testFetcher()
    {
        saveAtlas(this.rule.getAtlas(),
                INPUT + InputDependency.INPUT_DEPENDENCY_FOLDER_KEY
                        + "junctionRoundaboutInputDependency/" + COUNTRY + "/" + COUNTRY + "_"
                        + TILE1 + FileSuffix.ATLAS);
        saveAtlas(this.rule.getAtlas(),
                INPUT + "/" + COUNTRY + "/" + COUNTRY + "_" + TILE2 + FileSuffix.ATLAS);

        final ConfiguredAtlasFetcher junctionRoundaboutFetcher = ConfiguredAtlasFetcher
                .from("junctionRoundaboutFetcher", this.configuration);
        final Atlas junctionRoundaboutAtlas = junctionRoundaboutFetcher
                .getFetcher(INPUT, COUNTRY, ResourceFileSystem.simpleconfiguration())
                .apply(SlippyTile.forName(TILE1))
                .orElseThrow(() -> new CoreException("{} was not there!", TILE1));
        Assert.assertEquals(1, junctionRoundaboutAtlas.numberOfEdges());

        final ConfiguredAtlasFetcher justSubAtlasFetcher = ConfiguredAtlasFetcher
                .from("justSubAtlasFetcher", this.configuration);
        final Atlas justSubAtlasAtlas = justSubAtlasFetcher
                .getFetcher(INPUT, COUNTRY, ResourceFileSystem.simpleconfiguration())
                .apply(SlippyTile.forName(TILE2))
                .orElseThrow(() -> new CoreException("{} was not there!", TILE2));
        Assert.assertEquals(3, justSubAtlasAtlas.numberOfEdges());
    }

    @Test(expected = CoreException.class)
    public void testMissingDependency()
    {
        ConfiguredAtlasFetcher.from("missingInputDependencyFetcher", this.configuration);
    }

    private void saveAtlas(final Atlas atlas, final String path)
    {
        final ByteArrayResource resource = new ByteArrayResource();
        atlas.save(resource);
        ResourceFileSystem.addResource(path, resource);
    }
}
