package org.openstreetmap.atlas.mutator;

import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lcram
 */
public class MergeForgivenessPolicyIntegrationTest
{
    public static final String INPUT = "resource://test/input";
    public static final String BOUNDARY_FILE_NAME = "boundaries.txt.gz";
    public static final String SHARDING_FILE_NAME = "sharding.txt";
    public static final String BOUNDARY_META_NAME = "boundaries.meta";
    public static final String SHARDING_META_NAME = "sharding.meta";
    public static final String BOUNDARY_META_CONTENTS = "I am the boundaries meta-data";
    public static final String SHARDING_META_CONTENTS = "I am the sharding meta-data";
    public static final String BOUNDARY = INPUT + "/" + BOUNDARY_FILE_NAME;
    public static final String SHARDING = INPUT + "/" + SHARDING_FILE_NAME;
    public static final String BOUNDARY_META = INPUT + "/" + BOUNDARY_META_NAME;
    public static final String SHARDING_META = INPUT + "/" + SHARDING_META_NAME;
    public static final String MUTATIONS = "resource://test/mutations/mutations.json";
    public static final String OUTPUT = "resource://test/output/atlas";
    public static final String Z_6_X_32_Y_31 = "/AAA/AAA_6-32-31.atlas";
    public static final String Z_7_X_66_Y_62 = "/AAA/AAA_7-66-62.atlas";
    public static final String Z_7_X_66_Y_63 = "/AAA/AAA_7-66-63.atlas";

    private static final Logger logger = LoggerFactory.getLogger(AtlasMutatorIntegrationTest.class);

    @Rule
    public final MergeForgivenessPolicyIntegrationTestRule rule = new MergeForgivenessPolicyIntegrationTestRule();

    public static void addResource(final String path, final String name)
    {
        addResource(path, name, false);
    }

    public static void addResource(final String path, final Resource resource)
    {
        ResourceFileSystem.addResource(path, resource);
    }

    public static void addResource(final String path, final String name, final boolean gzipIt)
    {
        Resource input = new InputStreamResource(
                () -> AtlasMutatorIntegrationTest.class.getResourceAsStream(name));
        if (gzipIt)
        {
            final ByteArrayResource newInput = new ByteArrayResource();
            newInput.setCompressor(Compressor.GZIP);
            input.copyTo(newInput);
            input = newInput;
        }
        ResourceFileSystem.addResource(path, input);
    }

    public Atlas atlasForPath(final String path)
    {
        return new AtlasResourceLoader().load(forPath(path));
    }

    public Resource forPath(final String path)
    {
        return SparkJob.resource(path, ResourceFileSystem.simpleconfiguration());
    }

    public String[] getArguments()
    {
        final StringList sparkConfiguration = new StringList();
        sparkConfiguration().entrySet().stream()
                .forEach(entry -> sparkConfiguration.add(entry.getKey() + "=" + entry.getValue()));

        final StringList arguments = new StringList();
        arguments.add("-cluster=local");
        arguments.add("-atlas=" + INPUT);
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add("-countries=AAA");
        arguments.add("-mutatorConfigurationResource=" + MUTATIONS);
        arguments.add("-mutatorConfigurationJson={}");
        arguments.add("-copyShardingAndBoundaries=true");
        arguments.add("-sparkOptions=" + sparkConfiguration.join(","));

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }
        return args;
    }

    @Test
    public void mutate()
    {
        // Run the mutation job
        new AtlasMutator().runWithoutQuitting(getArguments());

        dump();

        final Atlas atlas1 = atlasForPath(OUTPUT + Z_6_X_32_Y_31);
        final Atlas atlas2 = atlasForPath(OUTPUT + Z_7_X_66_Y_62);
        final Atlas atlas3 = atlasForPath(OUTPUT + Z_7_X_66_Y_63);

        /*
         * We will use MultiAtlas for testing. We don't really care about testing proper shard
         * assignment here, since this test is largely for confirming that the merge conflict
         * resolution code is working.
         */
        final Atlas multi = new MultiAtlas(atlas1, atlas2, atlas3);

        /*
         * For this test, we configured mutations that introduce numerous conflicts. The types of
         * conflicts are:
         */
        /*
         * 1) AtlasChangeGeneratorSelfConflicting creates an internal ADD/REMOVE conflict, which
         * attempts to both add a 'foo=bar' tag to all Points while also removing all Points. We
         * utilize a mutation specific strategy to resolve this conflict by always choosing the
         * FeatureChange containing a 'foo=bar' tag (effectively throwing out the REMOVEs).
         */
        /*
         * 2) AtlasChangeGeneratorRemoveMeTag and AtlasChangeGeneratorRemovePointsWithRemoveMeTag
         * generate a conflict where AtlasChangeGeneratorRemoveMeTag simply tries to remove any
         * 'removeme=*' tag, while AtlasChangeGeneratorRemovePointsWithRemoveMeTag actually deletes
         * Points with a 'removeme=*' tag. Since this ADD/REMOVE conflict occurs between mutations,
         * it must be resolved globally. We attempt to resolve it by always choosing the REMOVE
         * FeatureChange.
         */
        /*
         * 3) The strategy we employ to mitigate conflict 1) actually causes a new ADD/REMOVE
         * conflict for any Point that has both a 'foo=bar' tag (introduced by resolution strategy
         * for AtlasChangeGeneratorSelfConflicting) and a 'removeme=*' tag.
         * AtlasChangeGeneratorRemovePointsWithRemoveMeTag will attempt to remove the point. Again,
         * we fall back to a global strategy which favors REMOVE FeatureChanges. This ultimately
         * causes us to drop Point 3 entirely, since it contains a 'removeme=*' tag.
         */
        Assert.assertNotNull(multi.node(1000000L));
        Assert.assertNotNull(multi.node(2000000L));
        // OSM node 3 should be purged from the atlas (it should appear as neither a Point or a
        // Node)
        Assert.assertNull(multi.point(3000000L));
        Assert.assertNull(multi.node(3000000L));
        // OSM node 4 and 5 should exist in atlas as Points with tags foo=bar
        Assert.assertEquals("bar", multi.point(4000000L).getTag("foo").orElse("FAIL"));
        Assert.assertEquals("bar", multi.point(5000000L).getTag("foo").orElse("FAIL"));
    }

    @Before
    public void setup()
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
        ResourceFileSystem.clear();

        addResource(SHARDING, "tree-6-14-100000.txt");
        addResource(BOUNDARY, "AAA.txt", true);
        addResource(MUTATIONS, "MergeForgivenessPolicyIntegrationTest.json");
        addResource(SHARDING_META, new StringResource(SHARDING_META_CONTENTS));
        addResource(BOUNDARY_META, new StringResource(BOUNDARY_META_CONTENTS));

        // Add the Atlas files to the resource file system
        final long size = 8192;
        final ByteArrayResource resource1 = new ByteArrayResource(size);
        final Atlas shard1 = this.rule.getZ6X32Y31();
        shard1.save(resource1);
        ResourceFileSystem.addResource(INPUT + Z_6_X_32_Y_31, resource1);
        final ByteArrayResource resource2 = new ByteArrayResource(size);
        final Atlas shard2 = this.rule.getZ7X66Y62();
        shard2.save(resource2);
        ResourceFileSystem.addResource(INPUT + Z_7_X_66_Y_62, resource2);
        final ByteArrayResource resource3 = new ByteArrayResource(size);
        final Atlas shard3 = this.rule.getZ7X66Y63();
        shard3.save(resource3);
        ResourceFileSystem.addResource(INPUT + Z_7_X_66_Y_63, resource3);
    }

    public Map<String, String> sparkConfiguration()
    {
        return ResourceFileSystem.simpleconfiguration();
    }

    void dump()
    {
        // This is used for local testing. If a developer adds a local file system path in this
        // environment variable, the result of the job will be entirely copied to the specified
        // path.
        final String resourceFileSystemDump = System.getenv("RESOURCE_FILE_SYSTEM_DUMP");
        if (resourceFileSystemDump != null && !resourceFileSystemDump.isEmpty())
        {
            final File folder = new File(resourceFileSystemDump);
            if (folder.exists())
            {
                folder.deleteRecursively();
            }
            folder.mkdirs();
            ResourceFileSystem.dumpToDisk(folder);
        }
    }
}
