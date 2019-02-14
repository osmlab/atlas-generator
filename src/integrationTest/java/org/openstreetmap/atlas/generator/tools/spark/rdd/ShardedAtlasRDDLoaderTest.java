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
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.builder.text.TextAtlasBuilder;
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.WritableResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * @author tian_du
 */
public class ShardedAtlasRDDLoaderTest extends SparkRDDTestBase implements Serializable
{

    private static Logger logger = LoggerFactory
            .getLogger(ShardedAtlasRDDLoaderTest.class.getCanonicalName());

    private final String country = "OMN";
    private final Set<String> sampleShardNames = new HashSet<>(
            Arrays.asList("7-84-54", "7-85-55", "6-42-28"));

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

    private List<Atlas> sampleInputAtlas;
    private CountryBoundaryMap boundaries;
    private Sharding atlasSharding;

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

        boundaries = CountryBoundaryMap.fromPlainText(
                FileSystemHelper.resource(BOUNDARIES_DIR + BOUNDARIES_FILE, fileSystemConfig));
        atlasSharding = AtlasSharding.forString("dynamic@" + SHARDING_DIR + SHARDING_FILE,
                fileSystemConfig);
        sampleInputAtlas = new ArrayList<>();

        sampleShardNames.forEach(shardName ->
        {
            try
            {
                final Atlas atlas = ShardedAtlasRDDLoader.loadOneAtlasShard(country, shardName,
                        TEST_BASE_DIR + "atlas", fileSystemConfig);
                sampleInputAtlas.add(atlas);
            }
            catch (final CoreException exception)
            {
                logger.error("exception loading atlas {}", shardName);
                exception.printStackTrace();
            }
        });

        // manually set the file system configuration to comply to ResourceFileSystem implementation
        setFileSystemConfiguration(fileSystemConfig);
    }

    @Test
    public void testShardedAtlasRDDLoading()
    {
        final JavaPairRDD<Shard, Atlas> shardAtlasPairRDD = ShardedAtlasRDDLoader
                .generateShardedAtlasRDD(getSparkContext(), country, boundaries,
                        TEST_BASE_DIR + "atlas", atlasSharding, getFileSystemConfiguration());
        final List<Atlas> atlasFromRDD = shardAtlasPairRDD.values().filter(Objects::nonNull)
                .collect();

        final MultiAtlas multiAtlasFromRDD = new MultiAtlas(atlasFromRDD);
        final MultiAtlas multiAtlas = new MultiAtlas(sampleInputAtlas);

        assertEquals(Iterables.size(multiAtlas.edges()), Iterables.size(multiAtlasFromRDD.edges()));
        assertEquals(Iterables.size(multiAtlas.areas()), Iterables.size(multiAtlasFromRDD.areas()));
        assertEquals(Iterables.size(multiAtlas.nodes()), Iterables.size(multiAtlasFromRDD.nodes()));
        assertEquals(multiAtlas.summary(), multiAtlasFromRDD.summary());

        logger.info(multiAtlas.summary());
        logger.info(multiAtlasFromRDD.summary());
    }

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
}
