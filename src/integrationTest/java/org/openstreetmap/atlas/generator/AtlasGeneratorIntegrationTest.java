package org.openstreetmap.atlas.generator;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.spark.persistence.PersistenceTools;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.compression.Decompressor;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.StringList;

/**
 * @author matthieun
 */
public class AtlasGeneratorIntegrationTest
{
    public static final String PBF = "resource://test/pbf";
    public static final String INPUT_SHARDING = PBF + "/" + PersistenceTools.SHARDING_FILE;
    public static final String INPUT_SHARDING_META = PBF + "/" + PersistenceTools.SHARDING_META;
    public static final String INPUT_BOUNDARIES = PBF + "/" + PersistenceTools.BOUNDARIES_FILE;
    public static final String INPUT_BOUNDARIES_META = PBF + "/" + PersistenceTools.BOUNDARIES_META;
    public static final String PBF_233 = PBF + "/9/9-168-233.osm.pbf";
    public static final String PBF_234 = PBF + "/9/9-168-234.osm.pbf";
    public static final String OUTPUT = "resource://test/output";
    public static final String ATLAS_OUTPUT = "resource://test/atlas";
    public static final String LINE_DELIMITED_GEOJSON_OUTPUT = "resource://test/"
            + AtlasGenerator.LINE_DELIMITED_GEOJSON_STATISTICS_FOLDER + "/DMA";
    public static final String CONFIGURED_OUTPUT_FILTER = "resource://test/filter/nothingFilter.json";
    public static final String FILTER_NAME = "nothingFilter";
    public static final String CONFIGURATION = "resource://test/configuration";
    public static final String EDGE_CONFIGURATION = CONFIGURATION + "/atlas-edge.json";

    static
    {
        addResource(PBF_233, "DMA_cutout.osm.pbf");
        addResource(PBF_234, "DMA_cutout.osm.pbf");
        addResource(INPUT_SHARDING, "tree-6-14-100000.txt");
        addResource(INPUT_BOUNDARIES, "DMA.txt", true);
        addResource(CONFIGURED_OUTPUT_FILTER, "nothingFilter.json");
        addResourceContents(INPUT_BOUNDARIES_META, "Meta data for boundaries");
        addResourceContents(INPUT_SHARDING_META, "Meta data for sharding");
        addResource(EDGE_CONFIGURATION, "atlas-edge.json", false, AtlasLoadingOption.class);
    }

    public static void addResource(final String path, final String name, final boolean gzipIt)
    {
        addResource(path, name, gzipIt, AtlasGeneratorIntegrationTest.class);
    }

    public static void addResource(final String path, final String name, final boolean gzipIt,
            final Class<?> clazz)
    {
        Resource input = new InputStreamResource(() -> clazz.getResourceAsStream(name));
        if (gzipIt)
        {
            final ByteArrayResource newInput = new ByteArrayResource();
            newInput.setCompressor(Compressor.GZIP);
            input.copyTo(newInput);
            input = newInput;
        }
        ResourceFileSystem.addResource(path, input);
    }

    private static void addResource(final String path, final String name)
    {
        addResource(path, name, false);
    }

    private static void addResourceContents(final String path, final String contents)
    {
        ResourceFileSystem.addResource(path, new StringResource(contents));
    }

    @Test
    public void testAtlasGeneration()
    {
        final StringList arguments = new StringList();
        arguments.add("-master=local");
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add("-countries=DMA");
        arguments.add("-pbfs=" + PBF);
        arguments.add("-pbfScheme=zz/zz-xx-yy.osm.pbf");
        arguments.add("-atlasScheme=zz/");
        arguments.add("-lineDelimitedGeojsonOutput=true");
        arguments.add("-copyShardingAndBoundaries=true");
        arguments.add("-configuredOutputFilter=" + CONFIGURED_OUTPUT_FILTER);
        arguments.add("-configuredFilterName=" + FILTER_NAME);
        arguments.add("-edgeConfiguration=" + EDGE_CONFIGURATION);
        arguments.add(
                "-sparkOptions=fs.resource.impl=" + ResourceFileSystem.class.getCanonicalName());

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }

        ResourceFileSystem.printContents();
        new AtlasGenerator().runWithoutQuitting(args);
        ResourceFileSystem.printContents();
        dump();

        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem())
        {
            Assert.assertTrue(resourceFileSystem
                    .exists(new Path(ATLAS_OUTPUT + "/DMA/9/DMA_9-168-233.atlas")));
            Assert.assertTrue(resourceFileSystem
                    .exists(new Path(ATLAS_OUTPUT + "/DMA/9/DMA_9-168-234.atlas")));
            Assert.assertTrue(resourceFileSystem.exists(
                    new Path(LINE_DELIMITED_GEOJSON_OUTPUT + "/9/DMA_9-168-233.ldgeojson.gz")));
            Assert.assertTrue(resourceFileSystem.exists(
                    new Path(LINE_DELIMITED_GEOJSON_OUTPUT + "/9/DMA_9-168-234.ldgeojson.gz")));
            Assert.assertEquals(402,
                    Iterables.size(resourceForName(resourceFileSystem,
                            LINE_DELIMITED_GEOJSON_OUTPUT + "/9/DMA_9-168-234.ldgeojson.gz")
                                    .lines()));
            Assert.assertEquals(336,
                    Iterables.size(resourceForName(resourceFileSystem,
                            LINE_DELIMITED_GEOJSON_OUTPUT + "/9/DMA_9-168-233.ldgeojson.gz")
                                    .lines()));

            Assert.assertTrue(resourceFileSystem.exists(new Path(
                    SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.SHARDING_FILE))));
            Assert.assertTrue(resourceFileSystem.exists(new Path(
                    SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.SHARDING_META))));
            Assert.assertTrue(resourceFileSystem.exists(new Path(
                    SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.BOUNDARIES_FILE))));
            Assert.assertTrue(resourceFileSystem.exists(new Path(
                    SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.BOUNDARIES_META))));

            Assert.assertTrue(resourceFileSystem.exists(
                    new Path("resource://test/configuredOutput/DMA/9/DMA_9-168-233.atlas")));
            Assert.assertTrue(resourceFileSystem.exists(
                    new Path("resource://test/configuredOutput/DMA/9/DMA_9-168-234.atlas")));
        }
        catch (IllegalArgumentException | IOException e)
        {
            throw new CoreException("Unable to find output Atlas files.", e);
        }
    }

    @Test
    public void testGeoHashingAtlasGeneration()
    {
        final StringList arguments = new StringList();
        arguments.add("-master=local");
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add("-countries=DMA");
        arguments.add("-pbfs=" + PBF);
        arguments.add("-pbfScheme=zz/zz-xx-yy.osm.pbf");
        arguments.add("-sharding=geohash@4");
        arguments.add("-lineDelimitedGeojsonOutput=true");
        arguments.add("-copyShardingAndBoundaries=true");
        arguments.add("-configuredOutputFilter=" + CONFIGURED_OUTPUT_FILTER);
        arguments.add("-configuredFilterName=" + FILTER_NAME);
        arguments.add(
                "-sparkOptions=fs.resource.impl=" + ResourceFileSystem.class.getCanonicalName());

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }

        ResourceFileSystem.printContents();
        new AtlasGenerator().runWithoutQuitting(args);
        ResourceFileSystem.printContents();
        dump();

        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem())
        {
            Assert.assertTrue(
                    resourceFileSystem.exists(new Path(ATLAS_OUTPUT + "/DMA/DMA_ddsq.atlas")));
            Assert.assertTrue(
                    resourceFileSystem.exists(new Path(ATLAS_OUTPUT + "/DMA/DMA_ddsr.atlas")));
            Assert.assertTrue(resourceFileSystem
                    .exists(new Path(LINE_DELIMITED_GEOJSON_OUTPUT + "/DMA_ddsq.ldgeojson.gz")));
            Assert.assertTrue(resourceFileSystem
                    .exists(new Path(LINE_DELIMITED_GEOJSON_OUTPUT + "/DMA_ddsr.ldgeojson.gz")));
            Assert.assertEquals(700, Iterables.size(resourceForName(resourceFileSystem,
                    LINE_DELIMITED_GEOJSON_OUTPUT + "/DMA_ddsq.ldgeojson.gz").lines()));
            Assert.assertEquals(2, Iterables.size(resourceForName(resourceFileSystem,
                    LINE_DELIMITED_GEOJSON_OUTPUT + "/DMA_ddsr.ldgeojson.gz").lines()));

            Assert.assertTrue(resourceFileSystem.exists(new Path(
                    SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.SHARDING_FILE))));
            Assert.assertTrue(resourceFileSystem.exists(new Path(
                    SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.SHARDING_META))));
            Assert.assertTrue(resourceFileSystem.exists(new Path(
                    SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.BOUNDARIES_FILE))));
            Assert.assertTrue(resourceFileSystem.exists(new Path(
                    SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.BOUNDARIES_META))));

            Assert.assertTrue(resourceFileSystem
                    .exists(new Path("resource://test/configuredOutput/DMA/DMA_ddsq.atlas")));
            Assert.assertTrue(resourceFileSystem
                    .exists(new Path("resource://test/configuredOutput/DMA/DMA_ddsr.atlas")));
        }
        catch (IllegalArgumentException | IOException e)
        {
            throw new CoreException("Unable to find output Atlas files.", e);
        }
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

    private Resource resourceForName(final ResourceFileSystem resourceFileSystem, final String name)
    {
        Decompressor decompressor = Decompressor.NONE;
        if (name.endsWith(FileSuffix.GZIP.toString()))
        {
            decompressor = Decompressor.GZIP;
        }
        return new InputStreamResource(() ->
        {
            try
            {
                return resourceFileSystem.open(new Path(name));
            }
            catch (final Exception e)
            {
                throw new CoreException("Unable to open Resource {} in ResourceFileSystem", name,
                        e);
            }
        }).withDecompressor(decompressor);
    }
}
