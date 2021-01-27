package org.openstreetmap.atlas.mutator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.tags.filters.TaggableFilter;
import org.openstreetmap.atlas.utilities.collections.StringList;

import com.google.common.collect.Iterables;

/**
 * @author matthieun
 */
public class AtlasMutatorUntangledIntegrationTest extends AtlasMutatorIntegrationTest
{
    public static final String OUTPUT = "resource://test/output/atlas/";
    public static final String OUTPUT_LEVEL = OUTPUT + AtlasMutationLevel.INTERMEDIATE_FOLDER_NAME
            + "/XYZ";
    public static final String OUTPUT_LEVEL_0 = OUTPUT_LEVEL + "_0";
    public static final String OUTPUT_LEVEL_1 = OUTPUT_LEVEL + "_1";
    public static final String GENERATED_XYZ_1_Z9X261Y195 = OUTPUT
            + "/intermediate/logs/XYZ-1-generated/XYZ-1-generated_9-261-195.geojson";

    @Override
    @Before
    public void before()
    {
        super.before();
    }

    public String[] getArguments(final boolean allLevels)
    {
        final StringList sparkConfiguration = new StringList();
        sparkConfiguration().entrySet().stream()
                .forEach(entry -> sparkConfiguration.add(entry.getKey() + "=" + entry.getValue()));

        final StringList arguments = new StringList();
        arguments.add("-cluster=local");
        arguments.add("-atlas=" + INPUT);
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add("-countries=XYZ");
        arguments.add("-levelIndex=" + (allLevels ? "1" : "0"));
        arguments.add("-runAllLevelsUpTo=" + allLevels);
        arguments.add("-shard=9-261-195");
        arguments.add("-countryShapes=" + BOUNDARY);
        arguments.add("-mutatorConfigurationResource=" + MUTATIONS);
        arguments.add("-mutatorConfigurationJson={}");
        arguments.add("-sparkOptions=" + sparkConfiguration.join(","));

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }
        return args;
    }

    @Override
    @Test
    public void mutate()
    {
        new AtlasMutatorUntangled().withSparkConfiguration(sparkConfiguration())
                .runWithoutQuitting(getArguments(false));

        dump();

        final Atlas l0z9x261y195 = atlasForPath(OUTPUT_LEVEL_0 + Z_9_X_261_Y_195);
        l0z9x261y195.entities().forEach(entity -> Assert.assertEquals(VALUE, entity.tag(KEY)));
    }

    @Test
    public void mutateZeroToOne()
    {
        final String[] arguments = getArguments(true);
        new AtlasMutatorUntangled().withSparkConfiguration(sparkConfiguration())
                .runWithoutQuitting(arguments);

        ResourceFileSystem.printContents();

        final Atlas l0z9x261y195 = atlasForPath(OUTPUT_LEVEL_0 + Z_9_X_261_Y_195);
        l0z9x261y195.entities().forEach(entity -> Assert.assertEquals(VALUE, entity.tag(KEY)));

        final Atlas l1z9x261y195 = atlasForPath(OUTPUT_LEVEL_1 + Z_9_X_261_Y_195);
        final TaggableFilter junctionRoundaboutFilter = TaggableFilter
                .forDefinition("junction->roundabout");
        l1z9x261y195
                // Everything but the new edge and node should still have the tag
                .entities(entity -> junctionRoundaboutFilter.negate().test(entity)
                        && entity.getIdentifier() > 100)
                .forEach(entity -> Assert.assertEquals(VALUE, entity.tag(KEY)));
        l1z9x261y195.entities(junctionRoundaboutFilter::test).forEach(entity ->
        {
            final Edge edge = (Edge) entity;
            Assert.assertNotEquals(edge.start(), edge.end());
        });

        final Resource generatedXYZ1Z9X261Y195 = forPath(GENERATED_XYZ_1_Z9X261Y195);
        Assert.assertEquals(4, Iterables.size(generatedXYZ1Z9X261Y195.lines()));
    }
}
