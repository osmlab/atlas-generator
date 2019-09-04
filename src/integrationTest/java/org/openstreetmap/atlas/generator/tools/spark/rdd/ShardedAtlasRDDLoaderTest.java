package org.openstreetmap.atlas.generator.tools.spark.rdd;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Ignore;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.builder.text.TextAtlasBuilder;
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.WritableResource;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.scalars.Distance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import scala.Tuple2;

/**
 * @author tian_du
 */
public class ShardedAtlasRDDLoaderTest extends SparkRDDTestBase implements Serializable
{

    private static Logger logger = LoggerFactory
            .getLogger(ShardedAtlasRDDLoaderTest.class.getCanonicalName());

    /**
     * artifacts
     */
    public static final String TEST_BASE_DIR = "resource://test/";
    public static final String ATLAS_SHARED_PATH_STRING = "atlas/OMN/";

    public static final String ATLAS_BASE_DIR = TEST_BASE_DIR + ATLAS_SHARED_PATH_STRING;

    public static final String OMN_ATLAS_ONE_FILE = "OMN_6-42-28.txt";
    public static final String OMN_ATLAS_TWO_FILE = "OMN_7-84-54.txt";
    public static final String OMN_ATLAS_THREE_FILE = "OMN_7-85-55.txt";
    public static final String OMN_ATLAS_ONE_FILE_NAME = "OMN_6-42-28.atlas";
    public static final String OMN_ATLAS_TWO_FILE_NAME = "OMN_7-84-54.atlas";
    public static final String OMN_ATLAS_THREE_FILE_NAME = "OMN_7-85-55.atlas";
    public static final String BOUNDARIES_SHARED_PATH_STRING = "boundaries/";
    public static final String BOUNDARIES_DIR = TEST_BASE_DIR + BOUNDARIES_SHARED_PATH_STRING;

    public static final String BOUNDARIES_FILE = "OMN_boundary.txt";
    public static final String SHARDING_SHARED_PATH_STRING = "sharding/";
    public static final String SHARDING_DIR = TEST_BASE_DIR + SHARDING_SHARED_PATH_STRING;

    public static final String SHARDING_FILE = "tree-6-14-100000.txt";
    static
    {
        addTextAtlasResource(ATLAS_BASE_DIR + OMN_ATLAS_ONE_FILE_NAME,
                ATLAS_SHARED_PATH_STRING + OMN_ATLAS_ONE_FILE);
        addTextAtlasResource(ATLAS_BASE_DIR + OMN_ATLAS_TWO_FILE_NAME,
                ATLAS_SHARED_PATH_STRING + OMN_ATLAS_TWO_FILE);
        addTextAtlasResource(ATLAS_BASE_DIR + OMN_ATLAS_THREE_FILE_NAME,
                ATLAS_SHARED_PATH_STRING + OMN_ATLAS_THREE_FILE);

        addResource(BOUNDARIES_DIR + BOUNDARIES_FILE,
                BOUNDARIES_SHARED_PATH_STRING + BOUNDARIES_FILE);
        addResource(SHARDING_DIR + SHARDING_FILE, SHARDING_SHARED_PATH_STRING + SHARDING_FILE);
    }
    private final String country = "OMN";

    private final Set<CountryShard> sampleCountryShards = new HashSet<>(
            Arrays.asList(new CountryShard(this.country, SlippyTile.forName("7-84-54")),
                    new CountryShard(this.country, SlippyTile.forName("7-85-55")),
                    new CountryShard(this.country, SlippyTile.forName("6-42-28"))));

    private List<Atlas> sampleInputAtlas;
    private CountryBoundaryMap boundaries;
    private Sharding atlasSharding;

    private static void addResource(final String path, final String name)
    {
        ResourceFileSystem.addResource(path, new InputStreamResource(
                () -> ShardedAtlasRDDLoaderTest.class.getResourceAsStream(name)));
    }

    private static void addTextAtlasResource(final String path, final String name)
    {
        final TextAtlasBuilder builder = new TextAtlasBuilder();
        final PackedAtlas packedAtlas = builder.read(new InputStreamResource(
                () -> ShardedAtlasRDDLoaderTest.class.getResourceAsStream(name)));
        final WritableResource packedAtlasResource = new ByteArrayResource();
        packedAtlas.save(packedAtlasResource);
        ResourceFileSystem.addResource(path, packedAtlasResource);
    }

    @Before
    public void setup()
    {
        final Map<String, String> fileSystemConfig = new HashMap<String, String>()
        {
            {
                put("fs.resource.impl", ResourceFileSystem.class.getCanonicalName());
                put("fs.file.impl", ResourceFileSystem.class.getName());
            }
        };

        this.boundaries = CountryBoundaryMap.fromPlainText(
                FileSystemHelper.resource(BOUNDARIES_DIR + BOUNDARIES_FILE, fileSystemConfig));
        this.atlasSharding = AtlasSharding.forString("dynamic@" + SHARDING_DIR + SHARDING_FILE,
                fileSystemConfig);
        this.sampleInputAtlas = new ArrayList<>();

        this.sampleCountryShards.forEach(countryShard ->
        {
            try
            {
                final Atlas atlas = ShardedAtlasRDDLoader.loadOneAtlasShard(
                        countryShard.getCountry(), countryShard.getShard().getName(),
                        TEST_BASE_DIR + "atlas", fileSystemConfig);
                this.sampleInputAtlas.add(atlas);
            }
            catch (final CoreException exception)
            {
                logger.error("exception loading atlas {}", countryShard);
                exception.printStackTrace();
            }
        });

        // manually set the file system configuration to comply to ResourceFileSystem implementation
        setFileSystemConfiguration(fileSystemConfig);
    }

    @Ignore
    public void testGenerateCountryShardedAtlasRDD()
    {
        // calling the method to generate the RDD
        final JavaPairRDD<CountryShard, List<Atlas>> countryShardedAtlasRDD = ShardedAtlasRDDLoader
                .generateCountryShardedAtlasRDD(getSparkContext(), new StringList(this.country),
                        this.boundaries, this.atlasSharding, TEST_BASE_DIR + "atlas",
                        getFileSystemConfiguration());

        // Collect RDD for assertion test
        final List<Tuple2<CountryShard, List<Atlas>>> countryShardedAtlas = countryShardedAtlasRDD
                .collect();

        // asserting the generated number of country sharded atlas should be the same as the sample
        // input, since the missing atlas will be filtered out
        assertEquals(countryShardedAtlas.size(), this.sampleCountryShards.size());
        for (final Tuple2<CountryShard, List<Atlas>> entry : countryShardedAtlas)
        {
            assertEquals(entry._2().size(), 1);
        }
    }

    @Ignore
    public void testGenerateExpandedCountryShardedAtlasRDD()
    {
        final Distance expandedDistance = Distance.meters(100);

        // calling the method to generate the RDD
        final JavaPairRDD<CountryShard, List<Atlas>> expandedCountryShardedAtlasRDD = ShardedAtlasRDDLoader
                .generateExpandedCountryShardedAtlasRDD(getSparkContext(), expandedDistance,
                        new StringList(this.country), this.boundaries, this.atlasSharding,
                        TEST_BASE_DIR + "atlas", getFileSystemConfiguration());

        // Manually generated the shard <-> expanded shards mapping
        final Map<CountryShard, Set<Shard>> shardWithExpandedShardList = new HashMap<>();
        for (final CountryShard countryShard : this.sampleCountryShards)
        {
            final Rectangle expandedBoundingBox = countryShard.getShard().bounds()
                    .expand(expandedDistance);
            final Set<Shard> expandedShards = Sets.newHashSet();
            this.atlasSharding.shards(expandedBoundingBox.bounds()).forEach(shard ->
            {
                // For testing purpose, only sample shards will be put into the map
                if (this.sampleCountryShards.contains(new CountryShard(this.country, shard)))
                {
                    expandedShards.add(shard);
                }
            });
            shardWithExpandedShardList.put(countryShard, expandedShards);
        }

        // Collect RDD for assertion test
        final List<Tuple2<CountryShard, List<Atlas>>> expandedCountryShardedAtlas = expandedCountryShardedAtlasRDD
                .collect();

        // Every central shard should have more than one atlas associated with it after expansion
        // The number of Atlas for each Shard in this test should be the number of available Atlas
        // from the sample test files
        for (final Map.Entry<CountryShard, Set<Shard>> entry : shardWithExpandedShardList
                .entrySet())
        {
            final CountryShard countryShard = entry.getKey();
            final Set<Shard> expandedShards = entry.getValue();
            assertEquals(shardWithExpandedShardList.get(countryShard).size(),
                    expandedShards.size());
        }
    }

    @Ignore
    public void testShardedAtlasRDDLoading()
    {
        final JavaPairRDD<Shard, Atlas> shardAtlasPairRDD = ShardedAtlasRDDLoader
                .generateShardedAtlasRDD(getSparkContext(), this.country, this.boundaries,
                        TEST_BASE_DIR + "atlas", this.atlasSharding, getFileSystemConfiguration());
        final List<Atlas> atlasFromRDD = shardAtlasPairRDD.values().filter(Objects::nonNull)
                .collect();

        final MultiAtlas multiAtlasFromRDD = new MultiAtlas(atlasFromRDD);
        final MultiAtlas multiAtlas = new MultiAtlas(this.sampleInputAtlas);

        assertEquals(Iterables.size(multiAtlas.edges()), Iterables.size(multiAtlasFromRDD.edges()));
        assertEquals(Iterables.size(multiAtlas.areas()), Iterables.size(multiAtlasFromRDD.areas()));
        assertEquals(Iterables.size(multiAtlas.nodes()), Iterables.size(multiAtlasFromRDD.nodes()));
        assertEquals(multiAtlas.summary(), multiAtlasFromRDD.summary());

        logger.info(multiAtlas.summary());
        logger.info(multiAtlasFromRDD.summary());
    }

}
