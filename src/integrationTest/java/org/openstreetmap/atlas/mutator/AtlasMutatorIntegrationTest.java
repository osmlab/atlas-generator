package org.openstreetmap.atlas.mutator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.items.Area;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.tags.filters.TaggableFilter;
import org.openstreetmap.atlas.utilities.collections.Iterables;

/**
 * @author matthieun
 */
public class AtlasMutatorIntegrationTest extends AbstractAtlasMutatorIntegrationTest
{
    public static final String GROUP0 = AtlasMutatorConfiguration.GROUP_PREFIX + "0";
    public static final String OUTPUT_XYZ_0_GENERATED_8_131_97_GEOJSON = OUTPUT + "/"
            + AtlasMutator.LOG_FOLDER + "/XYZ-0-generated/XYZ-0-generated_8-131-97.geojson.gz";
    public static final String OUTPUT_XYZ_0_ASSIGNED_8_131_97_GEOJSON = OUTPUT + "/"
            + AtlasMutator.LOG_FOLDER + "/XYZ-0-assigned/XYZ-0-assigned_8-131-97.geojson.gz";
    public static final String OUTPUT_XYZ_0_APPLIED_8_131_97_GEOJSON = OUTPUT + "/"
            + AtlasMutator.LOG_FOLDER + "/XYZ-0-applied/XYZ-0-applied_8-131-97.geojson.gz";
    public static final String OUTPUT_XYZ_2_APPLIED_8_131_97_GEOJSON = OUTPUT + "/"
            + AtlasMutator.LOG_FOLDER + "/XYZ-2-applied/XYZ-2-applied_8-131-97.geojson.gz";
    public static final String OUTPUT_JKL_1_APPLIED_8_131_97_GEOJSON = OUTPUT + "/"
            + AtlasMutator.LOG_FOLDER + "/JKL-1-applied/JKL-1-applied_8-131-97.geojson.gz";
    public static final String OUTPUT_LEVEL_1 = OUTPUT + "/"
            + AtlasMutationLevel.INTERMEDIATE_FOLDER_NAME + "/" + GROUP0 + "_1";
    public static final String OUTPUT_LEVEL_2 = OUTPUT + "/"
            + AtlasMutationLevel.INTERMEDIATE_FOLDER_NAME + "/" + GROUP0 + "_2";
    public static final String INPUT_DEPENDENCY = OUTPUT + "/"
            + AtlasMutationLevel.INTERMEDIATE_FOLDER_NAME + "/" + GROUP0
            + "_3_inputDependency_edgeNodeInputDependency";
    public static final String Z_8_X_130_Y_98 = "/XYZ/XYZ_8-130-98.atlas";
    public static final String Z_8_X_131_Y_97 = "/XYZ/XYZ_8-131-97.atlas";
    public static final String Z_8_X_131_Y_98 = "/XYZ/XYZ_8-131-98.atlas";
    public static final String Z_9_X_261_Y_195 = "/XYZ/XYZ_9-261-195.atlas";
    public static final String JKL_Z_9_X_261_Y_195 = "/JKL/JKL_9-261-195.atlas";
    public static final String ABC_Z_9_X_261_Y_195 = "/ABC/ABC_9-261-195.atlas";
    public static final String OUTPUT_LOG_PREFIX = OUTPUT + "/XYZ-";
    public static final String OUTPUT_POTENTIAL_LOG_FILES_XYZ = OUTPUT + "/"
            + AtlasMutator.POTENTIAL_LOG_FILES_PATH + "/XYZ"
            + AtlasMutator.POTENTIAL_LOG_FILES_NAME;
    public static final String OUTPUT_POTENTIAL_LOG_FILES_JKL = OUTPUT + "/"
            + AtlasMutator.POTENTIAL_LOG_FILES_PATH + "/JKL"
            + AtlasMutator.POTENTIAL_LOG_FILES_NAME;
    public static final String OUTPUT_POTENTIAL_LOG_FILES_ABC = OUTPUT + "/"
            + AtlasMutator.POTENTIAL_LOG_FILES_PATH + "/ABC"
            + AtlasMutator.POTENTIAL_LOG_FILES_NAME;

    @Rule
    public final AtlasMutatorIntegrationTestRule rule = new AtlasMutatorIntegrationTestRule();

    @Override
    public List<String> arguments()
    {
        final List<String> arguments = new ArrayList<>();
        arguments.add("-" + AtlasMutatorParameters.SUBMISSION_STAGGERING_WAIT.getName() + "=0.01");
        arguments.add("-" + AtlasGeneratorParameters.COUNTRIES.getName() + "=XYZ,JKL,ABC");
        arguments.add("-" + AtlasMutatorParameters.DEBUG_MUTATIONS.getName()
                + "=AddMutatedEqualsYes,AddSparkConfigurationTag,"
                + "AtlasChangeGeneratorSplitRoundabout,AtlasChangeGeneratorRemoveReverseEdges,"
                + "AddTurnRestrictions,AddMutatedAgainEqualsYes,AddSomeOtherTag,AddYetSomeOtherTag,"
                + "AddGeoHashName,AddSlippyName");
        arguments.add("-" + AtlasMutatorParameters.ALLOW_RDD.getName() + "=true");
        arguments.add("-" + AtlasMutatorParameters.PRELOAD_RDD.getName() + "=true");
        arguments.add("-" + AtlasMutatorParameters.DEBUG_SHARDS.getName()
                + "=8-130-98,8-131-97,8-131-98,9-261-195");
        return arguments;
    }

    @Before
    public void before()
    {
        setup("AtlasMutatorIntegrationTest.json", "tree-6-14-100000.txt",
                "AtlasMutatorIntegrationTestBoundaries.txt");

        // Add the Atlas files to the resource file system
        final long size = 8192;
        final ByteArrayResource resource1 = new ByteArrayResource(size);
        final Atlas shard1 = this.rule.getZ9X261Y195();
        shard1.save(resource1);
        ResourceFileSystem.addResource(INPUT + Z_9_X_261_Y_195, resource1);
        ResourceFileSystem.addResource(INPUT + JKL_Z_9_X_261_Y_195, resource1);
        ResourceFileSystem.addResource(INPUT + ABC_Z_9_X_261_Y_195, resource1);
        final ByteArrayResource resource2 = new ByteArrayResource(size);
        final Atlas shard2 = this.rule.getZ8X131Y97();
        shard2.save(resource2);
        ResourceFileSystem.addResource(INPUT + Z_8_X_131_Y_97, resource2);
        final ByteArrayResource resource3 = new ByteArrayResource(size);
        final Atlas shard3 = this.rule.getZ8X131Y98();
        shard3.save(resource3);
        ResourceFileSystem.addResource(INPUT + Z_8_X_131_Y_98, resource3);
    }

    @Test
    public void mutate()
    {
        runWithoutQuitting();

        dump();

        ////////////////
        // Meta-Data checks
        ////////////////

        // Check output country and levels
        final Resource countryAndLevels = forPath(OUTPUT_COUNTRY_LIST);
        final Resource countryAndLevelsReference = new InputStreamResource(
                () -> AtlasMutatorIntegrationTest.class
                        .getResourceAsStream(AtlasMutator.COUNTRY_AND_LEVELS));
        Assert.assertEquals(countryAndLevelsReference.all(), countryAndLevels.all());

        // Check the potential log files
        final Set<String> potentialPaths = Iterables
                .asSet(forPath(OUTPUT_POTENTIAL_LOG_FILES_XYZ).lines());
        final Set<String> potentialPathsReference = Iterables
                .asSet(new InputStreamResource(() -> AtlasMutatorIntegrationTest.class
                        .getResourceAsStream("XYZ_potentialLogFiles.txt")).lines());
        Assert.assertEquals(potentialPathsReference, potentialPaths);
        Assert.assertEquals(20, Iterables.size(forPath(OUTPUT_POTENTIAL_LOG_FILES_JKL).lines()));
        Assert.assertEquals(16, Iterables.size(forPath(OUTPUT_POTENTIAL_LOG_FILES_ABC).lines()));
        final List<String> outputLogs = listPaths(OUTPUT_LOG_PREFIX).stream()
                .filter(value -> value.contains(AtlasMutator.LOG_APPLIED))
                .collect(Collectors.toList());
        final Set<String> fullPotentialPaths = potentialPaths.stream()
                .map(finalPath -> SparkFileHelper.combine(OUTPUT, finalPath))
                .collect(Collectors.toSet());
        outputLogs.forEach(outputLog -> Assert.assertTrue(
                "Actual log file " + outputLog
                        + " was not included in the list of potential log files.",
                fullPotentialPaths.contains(outputLog)));

        // Check output boundary and sharding
        Assert.assertEquals(3, CountryBoundaryMap.fromPlainText(forPath(OUTPUT_BOUNDARIES))
                .allCountryNames().size());
        Assert.assertEquals(14290, Iterables.size(
                AtlasSharding.forString(OUTPUT_SHARDING_NAME, sparkConfiguration()).shards()));
        Assert.assertEquals(SHARDING_META_CONTENTS,
                forPath(OUTPUT_SHARDING_META).lines().iterator().next());
        Assert.assertEquals(BOUNDARY_META_CONTENTS,
                forPath(OUTPUT_BOUNDARIES_META).lines().iterator().next());

        ////////////////
        // Data checks
        ////////////////

        // Make sure input dependency is there
        final Atlas l3dz9x261y195 = atlasForPath(INPUT_DEPENDENCY + Z_9_X_261_Y_195);
        Assert.assertEquals(5, l3dz9x261y195.numberOfEdges());
        Assert.assertEquals(7, l3dz9x261y195.numberOfNodes());
        Assert.assertEquals(0, l3dz9x261y195.numberOfAreas());
        Assert.assertEquals(0, l3dz9x261y195.numberOfLines());
        Assert.assertEquals(0, l3dz9x261y195.numberOfPoints());

        // Validate L1 output
        // This one is new!
        final Atlas l1z8x130y98 = atlasForPath(OUTPUT_LEVEL_1 + Z_8_X_130_Y_98);
        Assert.assertEquals(1, l1z8x130y98.numberOfEdges());
        Assert.assertEquals(2, l1z8x130y98.numberOfNodes());
        Assert.assertEquals(1, l1z8x130y98.numberOfAreas());
        l1z8x130y98.entities().forEach(entity -> Assert.assertEquals(VALUE, entity.tag(KEY)));
        l1z8x130y98.entities().forEach(entity -> Assert.assertTrue(entity.tag("sparkConfiguration")
                .contains(ResourceFileSystem.RESOURCE_FILE_SYSTEM_CONFIGURATION)));

        // Those already existed
        final Atlas l1z9x261y195 = atlasForPath(OUTPUT_LEVEL_1 + Z_9_X_261_Y_195);
        Assert.assertEquals(5, l1z9x261y195.numberOfEdges());
        Assert.assertEquals(7, l1z9x261y195.numberOfNodes());
        Assert.assertEquals(2, l1z9x261y195.numberOfAreas());
        l1z9x261y195.entities().forEach(entity ->
        {
            if (entity.getIdentifier() != 1)
            {
                // This shard has one new node at level 2 that does not have mutated=yes
                Assert.assertEquals(VALUE, entity.tag(KEY));
            }
        });
        final Area forest = l1z9x261y195
                .areas(area -> TaggableFilter.forDefinition("landuse->forest").test(area))
                .iterator().next();
        Assert.assertEquals("snfuzg,snfu", forest.tag("geohashes"));
        Assert.assertEquals("4-8-6,6-32-24", forest.tag("slippytiles"));

        final Atlas l1z8x131y97 = atlasForPath(OUTPUT_LEVEL_1 + Z_8_X_131_Y_97);
        Assert.assertEquals(4, l1z8x131y97.numberOfEdges());
        Assert.assertEquals(5, l1z8x131y97.numberOfNodes());
        Assert.assertEquals(1, l1z8x131y97.numberOfAreas());
        Assert.assertEquals(1, l1z8x131y97.numberOfLines());
        l1z8x131y97.entities().forEach(entity ->
        {
            if (entity.getIdentifier() != 1)
            {
                // This shard has one new node at level 2 that does not have mutated=yes
                Assert.assertEquals(VALUE, entity.tag(KEY));
                Assert.assertEquals("0", entity.tag(AtlasMutator.MUTATOR_META_DATA_KEY
                        + AtlasMutator.MUTATOR_META_DATA_SPLIT + "AddMutatedEqualsYes"));
            }
        });

        final Atlas l1z8x131y98 = atlasForPath(OUTPUT_LEVEL_1 + Z_8_X_131_Y_98);
        Assert.assertEquals(2, l1z8x131y98.numberOfEdges());
        Assert.assertEquals(4, l1z8x131y98.numberOfNodes());
        Assert.assertEquals(2, l1z8x131y98.numberOfAreas());
        Assert.assertEquals(1, l1z8x131y98.numberOfLines());
        l1z8x131y98.entities().forEach(entity -> Assert.assertEquals(VALUE, entity.tag(KEY)));

        // Make sure pre-RDDLEVEL level output is not saved
        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem())
        {
            Assert.assertTrue(resourceFileSystem.isDirectory(new Path(OUTPUT_LEVEL_1)));
            Assert.assertFalse(resourceFileSystem.isDirectory(new Path(OUTPUT_LEVEL_2)));
        }
        catch (final IOException e)
        {
            throw new CoreException("isDirectory failed", e);
        }

        // Validate final output
        final Atlas outputLz9x261y195 = atlasForPath(OUTPUT + Z_9_X_261_Y_195);
        Assert.assertEquals(3, outputLz9x261y195.numberOfRelations());
        Assert.assertEquals(5, outputLz9x261y195.numberOfEdges());
        Assert.assertEquals(7, outputLz9x261y195.numberOfNodes());
        Assert.assertEquals(2, outputLz9x261y195.numberOfAreas());
        outputLz9x261y195.entities().forEach(
                entity -> Assert.assertEquals("someOtherValue", entity.tag("someOtherTag")));
        /*
         * This metadata should be empty, since all the relation changes are brand new Relation
         * ADDs. In the case of brand new relation ADDs, we are now leaving the "mutator:" tags on
         * the relation itself instead of moving them to the metadata.
         */
        Assert.assertTrue(outputLz9x261y195.metaData().getTags().isEmpty());
        outputLz9x261y195.relations().forEach(
                relation -> Assert.assertTrue(relation.getTags().entrySet().stream().anyMatch(
                        entry -> entry.getKey().startsWith(AtlasMutator.MUTATOR_META_DATA_KEY
                                + AtlasMutator.MUTATOR_META_DATA_SPLIT))));

        // Debug Geojson
        final Resource xyz0Generated813197Geojson = forPath(
                OUTPUT_XYZ_0_GENERATED_8_131_97_GEOJSON);
        Assert.assertEquals(36, Iterables.size(xyz0Generated813197Geojson.lines()));
        Assert.assertTrue(
                xyz0Generated813197Geojson.all().contains(AtlasMutator.MUTATOR_META_DATA_KEY));
        final Resource xyz0Assigned813197Geojson = forPath(OUTPUT_XYZ_0_ASSIGNED_8_131_97_GEOJSON);
        Assert.assertEquals(80, Iterables.size(xyz0Assigned813197Geojson.lines()));
        final Resource xyz0Applied813197Geojson = forPath(OUTPUT_XYZ_0_APPLIED_8_131_97_GEOJSON);
        Assert.assertEquals(9, Iterables.size(xyz0Applied813197Geojson.lines()));
        // Make sure that all the applied geojson lines are from mutations, and not from side effect
        // removals from ChangeFilter
        final Resource jkl1Applied813197Geojson = forPath(OUTPUT_JKL_1_APPLIED_8_131_97_GEOJSON);
        Assert.assertEquals(5, Iterables.size(jkl1Applied813197Geojson.lines()));
        jkl1Applied813197Geojson.lines()
                .forEach(line -> Assert.assertTrue(
                        "Line does not contain mutation meta-data: " + line,
                        line.contains("\"metadata\":{\"mutator\":")));

        // Make sure that even for tag changes, we have the beforeView geometry
        final Resource xyz2Applied813197Geojson = forPath(OUTPUT_XYZ_2_APPLIED_8_131_97_GEOJSON);
        // The landuse=residential (Area)
        Assert.assertTrue(xyz2Applied813197Geojson.all()
                .contains("[[[4.2192121,38.8229598],[4.2182248,38.8227942],[4.2189334,38.822044],"
                        + "[4.2190847,38.8222539],[4.219474,38.8221216],[4.2197147,38.822389],"
                        + "[4.2194882,38.8225738],[4.2194705,38.8229267],[4.2192121,38.8229598]]]"));
        // The waterway=river (Line)
        Assert.assertTrue(xyz2Applied813197Geojson.all()
                .contains("[[4.2209701,38.8229134],[4.2218563,38.8222356],[4.2232792,38.8229514],"
                        + "[4.2249136,38.8220962],[4.2260682,38.8229134]]"));
        // New node
        Assert.assertTrue(xyz2Applied813197Geojson.all().contains("[4.2188444,38.8250936]"));
    }
}
