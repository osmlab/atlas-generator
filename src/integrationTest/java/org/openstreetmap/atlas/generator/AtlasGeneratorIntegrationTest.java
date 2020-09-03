package org.openstreetmap.atlas.generator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.generator.tools.spark.persistence.PersistenceTools;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
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

    public static final String DMA_233 = "/DMA/9/DMA_9-168-233";
    public static final String DMA_234 = "/DMA/9/DMA_9-168-234";
    public static final String DMA_DDSQ = "/DMA/DMA_ddsq";
    public static final String DMA_DDSR = "/DMA/DMA_ddsr";

    public static final String ATLAS_OUTPUT = "resource://test/atlas";
    public static final String ATLAS_OUTPUT_233 = ATLAS_OUTPUT + DMA_233
            + FileSuffix.ATLAS.toString();
    public static final String ATLAS_OUTPUT_234 = ATLAS_OUTPUT + DMA_234
            + FileSuffix.ATLAS.toString();
    public static final String ATLAS_OUTPUT_DDSQ = ATLAS_OUTPUT + DMA_DDSQ
            + FileSuffix.ATLAS.toString();
    public static final String ATLAS_OUTPUT_DDSR = ATLAS_OUTPUT + DMA_DDSR
            + FileSuffix.ATLAS.toString();

    public static final String LDGEOJSON_GZ = ".ldgeojson.gz";
    public static final String LINE_DELIMITED_GEOJSON_OUTPUT = "resource://test/"
            + AtlasGenerator.LINE_DELIMITED_GEOJSON_STATISTICS_FOLDER;
    public static final String LINE_DELIMITED_GEOJSON_OUTPUT_233 = LINE_DELIMITED_GEOJSON_OUTPUT
            + DMA_233 + LDGEOJSON_GZ;
    public static final String LINE_DELIMITED_GEOJSON_OUTPUT_234 = LINE_DELIMITED_GEOJSON_OUTPUT
            + DMA_234 + LDGEOJSON_GZ;
    public static final String LINE_DELIMITED_GEOJSON_OUTPUT_DDSQ = LINE_DELIMITED_GEOJSON_OUTPUT
            + DMA_DDSQ + LDGEOJSON_GZ;
    public static final String LINE_DELIMITED_GEOJSON_OUTPUT_DDSR = LINE_DELIMITED_GEOJSON_OUTPUT
            + DMA_DDSR + LDGEOJSON_GZ;

    public static final String COUNTRY_STATS = "resource://test/countryStats";
    public static final String SHARD_STATS = "resource://test/shardStats";
    public static final String CONFIGURED_OUTPUT_FILTER = "resource://test/filter/nothingFilter.json";
    public static final String CONFIGURED_OUTPUT_FILTER_2 = "resource://test/filter/highwayFilter.json";
    public static final String FILTER_NAME = "nothingFilter";
    public static final String FILTER_NAME_2 = "highwayFilter";
    public static final String CONFIGURATION = "resource://test/configuration";
    public static final String EDGE_CONFIGURATION = CONFIGURATION + "/atlas-edge.json";
    public static final String WAY_SECTIONING_CONFIGURATION = CONFIGURATION
            + "/atlas-way-section.json";
    public static final String PBF_NODE_CONFIGURATION = CONFIGURATION + "/osm-pbf-node.json";
    public static final String PBF_WAY_CONFIGURATION = CONFIGURATION + "/osm-pbf-way.json";
    public static final String PBF_RELATION_CONFIGURATION = CONFIGURATION
            + "/osm-pbf-relation.json";
    public static final String SLICING_CONFIGURATION = CONFIGURATION
            + "/atlas-relation-slicing.json";

    public static final String CONFIGURED_OUTPUT = "resource://test/configuredOutput/";
    public static final String CONFIGURED_OUTPUT_FILTER_NAME_233 = CONFIGURED_OUTPUT + FILTER_NAME
            + DMA_233;
    public static final String CONFIGURED_OUTPUT_FILTER_NAME_234 = CONFIGURED_OUTPUT + FILTER_NAME
            + DMA_234;
    public static final String CONFIGURED_OUTPUT_FILTER_NAME_2_233 = CONFIGURED_OUTPUT
            + FILTER_NAME_2 + DMA_233;
    public static final String CONFIGURED_OUTPUT_FILTER_NAME_2_234 = CONFIGURED_OUTPUT
            + FILTER_NAME_2 + DMA_234;
    public static final String CONFIGURED_OUTPUT_FILTER_NAME_DDSQ = CONFIGURED_OUTPUT + FILTER_NAME
            + DMA_DDSQ;
    public static final String CONFIGURED_OUTPUT_FILTER_NAME_DDSR = CONFIGURED_OUTPUT + FILTER_NAME
            + DMA_DDSR;
    public static final String CONFIGURED_OUTPUT_FILTER_NAME_2_DDSQ = CONFIGURED_OUTPUT
            + FILTER_NAME_2 + DMA_DDSQ;
    public static final String CONFIGURED_OUTPUT_FILTER_NAME_2_DDSR = CONFIGURED_OUTPUT
            + FILTER_NAME_2 + DMA_DDSR;

    @After
    public void clean()
    {
        ResourceFileSystem.clear();
    }

    @Before
    public void setup()
    {
        clean();
        ResourceFileSystem.registerResourceExtractionClass(AtlasGeneratorIntegrationTest.class);
        ResourceFileSystem.addResource(PBF_233, "DMA_cutout.osm.pbf");
        ResourceFileSystem.addResource(PBF_234, "DMA_cutout.osm.pbf");
        ResourceFileSystem.addResource(INPUT_SHARDING, "tree-6-14-100000.txt");
        ResourceFileSystem.addResource(INPUT_BOUNDARIES, "DMA.txt", true);
        ResourceFileSystem.addResource(CONFIGURED_OUTPUT_FILTER, "nothingFilter.json");
        ResourceFileSystem.addResource(CONFIGURED_OUTPUT_FILTER_2, "highwayFilter.json");
        ResourceFileSystem.addResourceContents(INPUT_BOUNDARIES_META, "Meta data for boundaries");
        ResourceFileSystem.addResourceContents(INPUT_SHARDING_META, "Meta data for sharding");
        ResourceFileSystem.addResource(EDGE_CONFIGURATION, "atlas-edge.json", false,
                AtlasLoadingOption.class);
        ResourceFileSystem.addResource(WAY_SECTIONING_CONFIGURATION, "atlas-way-section.json",
                false, AtlasLoadingOption.class);
        ResourceFileSystem.addResource(PBF_NODE_CONFIGURATION, "osm-pbf-node.json", false,
                AtlasLoadingOption.class);
        ResourceFileSystem.addResource(PBF_WAY_CONFIGURATION, "osm-pbf-way.json", false,
                AtlasLoadingOption.class);
        ResourceFileSystem.addResource(PBF_RELATION_CONFIGURATION, "osm-pbf-relation.json", false,
                AtlasLoadingOption.class);
        ResourceFileSystem.addResource(SLICING_CONFIGURATION, "atlas-relation-slicing.json", false,
                AtlasLoadingOption.class);
    }

    @Test
    public void testAtlasGeneration()
    {
        final StringList sparkConfiguration = new StringList();
        ResourceFileSystem.simpleconfiguration().entrySet()
                .forEach(entry -> sparkConfiguration.add(entry.getKey() + "=" + entry.getValue()));

        final StringList arguments = new StringList();
        arguments.add("-cluster=local");
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add("-countries=DMA");
        arguments.add("-pbfs=" + PBF);
        arguments.add("-pbfScheme=zz/zz-xx-yy.osm.pbf");
        arguments.add("-atlasScheme=zz/");
        arguments.add("-lineDelimitedGeojsonOutput=true");
        arguments.add("-copyShardingAndBoundaries=true");
        arguments.add("-statistics=true");
        arguments.add("-configuredOutputFilter=" + CONFIGURED_OUTPUT_FILTER + ","
                + CONFIGURED_OUTPUT_FILTER_2);
        arguments.add("-configuredFilterName=" + FILTER_NAME + "," + FILTER_NAME_2);
        arguments.add("-" + AtlasGeneratorParameters.EDGE_CONFIGURATION.getName() + "="
                + EDGE_CONFIGURATION);
        arguments.add("-" + AtlasGeneratorParameters.WAY_SECTIONING_CONFIGURATION.getName() + "="
                + WAY_SECTIONING_CONFIGURATION);
        arguments.add("-" + AtlasGeneratorParameters.PBF_NODE_CONFIGURATION.getName() + "="
                + PBF_NODE_CONFIGURATION);
        arguments.add("-" + AtlasGeneratorParameters.PBF_WAY_CONFIGURATION.getName() + "="
                + PBF_WAY_CONFIGURATION);
        arguments.add("-" + AtlasGeneratorParameters.PBF_RELATION_CONFIGURATION.getName() + "="
                + PBF_RELATION_CONFIGURATION);
        arguments.add("-" + AtlasGeneratorParameters.SLICING_CONFIGURATION.getName() + "="
                + SLICING_CONFIGURATION);
        arguments.add("-" + SparkJob.SPARK_OPTIONS.getName() + "=" + sparkConfiguration.join(","));

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }

        ResourceFileSystem.printContents();
        new AtlasGenerator().runWithoutQuitting(args);
        ResourceFileSystem.printContents();
        dump();

        Assert.assertTrue(ResourceFileSystem.getResource(ATLAS_OUTPUT_233).isPresent());
        Assert.assertTrue(ResourceFileSystem.getResource(ATLAS_OUTPUT_234).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(LINE_DELIMITED_GEOJSON_OUTPUT_233).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(LINE_DELIMITED_GEOJSON_OUTPUT_234).isPresent());
        Assert.assertEquals(402, Iterables.size(
                ResourceFileSystem.getResourceOrElse(LINE_DELIMITED_GEOJSON_OUTPUT_234).lines()));
        Assert.assertEquals(336, Iterables.size(
                ResourceFileSystem.getResourceOrElse(LINE_DELIMITED_GEOJSON_OUTPUT_233).lines()));

        Assert.assertTrue(ResourceFileSystem
                .getResource(SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.SHARDING_FILE))
                .isPresent());
        Assert.assertTrue(ResourceFileSystem
                .getResource(SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.SHARDING_META))
                .isPresent());
        Assert.assertTrue(ResourceFileSystem
                .getResource(
                        SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.BOUNDARIES_FILE))
                .isPresent());
        Assert.assertTrue(ResourceFileSystem
                .getResource(
                        SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.BOUNDARIES_META))
                .isPresent());

        Assert.assertTrue(
                ResourceFileSystem.getResource(CONFIGURED_OUTPUT_FILTER_NAME_233).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(CONFIGURED_OUTPUT_FILTER_NAME_234).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(CONFIGURED_OUTPUT_FILTER_NAME_2_233).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(CONFIGURED_OUTPUT_FILTER_NAME_2_234).isPresent());

        final String countryStats = ResourceFileSystem
                .getResourceOrElse(COUNTRY_STATS + "/DMA.csv.gz").all();
        final String shard933Stats = ResourceFileSystem
                .getResourceOrElse(SHARD_STATS + "/DMA/9/DMA_9-168-233.csv.gz").all();
        final String shard934Stats = ResourceFileSystem
                .getResourceOrElse(SHARD_STATS + "/DMA/9/DMA_9-168-234.csv.gz").all();
        Assert.assertNotEquals(countryStats, shard933Stats);
        Assert.assertNotEquals(countryStats, shard934Stats);
    }

    @Test
    public void testGeoHashingAtlasGeneration()
    {
        final StringList sparkConfiguration = new StringList();
        ResourceFileSystem.simpleconfiguration().entrySet()
                .forEach(entry -> sparkConfiguration.add(entry.getKey() + "=" + entry.getValue()));

        final StringList arguments = new StringList();
        arguments.add("-cluster=local");
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add("-countries=DMA");
        arguments.add("-pbfs=" + PBF);
        arguments.add("-pbfScheme=zz/zz-xx-yy.osm.pbf");
        arguments.add("-sharding=geohash@4");
        arguments.add("-lineDelimitedGeojsonOutput=true");
        arguments.add("-copyShardingAndBoundaries=true");
        arguments.add("-configuredOutputFilter=" + CONFIGURED_OUTPUT_FILTER + ","
                + CONFIGURED_OUTPUT_FILTER_2);
        arguments.add("-configuredFilterName=" + FILTER_NAME + "," + FILTER_NAME_2);
        arguments.add("-" + SparkJob.SPARK_OPTIONS.getName() + "=" + sparkConfiguration.join(","));

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }

        ResourceFileSystem.printContents();
        new AtlasGenerator().runWithoutQuitting(args);
        ResourceFileSystem.printContents();
        dump();

        Assert.assertTrue(ResourceFileSystem.getResource(ATLAS_OUTPUT_DDSQ).isPresent());
        Assert.assertTrue(ResourceFileSystem.getResource(ATLAS_OUTPUT_DDSR).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(LINE_DELIMITED_GEOJSON_OUTPUT_DDSQ).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(LINE_DELIMITED_GEOJSON_OUTPUT_DDSR).isPresent());
        Assert.assertEquals(700, Iterables.size(
                ResourceFileSystem.getResourceOrElse(LINE_DELIMITED_GEOJSON_OUTPUT_DDSQ).lines()));
        Assert.assertEquals(2, Iterables.size(
                ResourceFileSystem.getResourceOrElse(LINE_DELIMITED_GEOJSON_OUTPUT_DDSR).lines()));

        Assert.assertTrue(ResourceFileSystem
                .getResource(SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.SHARDING_FILE))
                .isPresent());
        Assert.assertTrue(ResourceFileSystem
                .getResource(SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.SHARDING_META))
                .isPresent());
        Assert.assertTrue(ResourceFileSystem
                .getResource(
                        SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.BOUNDARIES_FILE))
                .isPresent());
        Assert.assertTrue(ResourceFileSystem
                .getResource(
                        SparkFileHelper.combine(ATLAS_OUTPUT, PersistenceTools.BOUNDARIES_META))
                .isPresent());

        Assert.assertTrue(
                ResourceFileSystem.getResource(CONFIGURED_OUTPUT_FILTER_NAME_DDSQ).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(CONFIGURED_OUTPUT_FILTER_NAME_DDSR).isPresent());
        Assert.assertTrue(
                ResourceFileSystem.getResource(CONFIGURED_OUTPUT_FILTER_NAME_2_DDSQ).isPresent());
        Assert.assertFalse(
                ResourceFileSystem.getResource(CONFIGURED_OUTPUT_FILTER_NAME_2_DDSR).isPresent());
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
