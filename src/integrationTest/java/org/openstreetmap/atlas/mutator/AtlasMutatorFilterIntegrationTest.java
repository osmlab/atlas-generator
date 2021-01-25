package org.openstreetmap.atlas.mutator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.ChangeType;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.mutator.filtering.ChangeFilter;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.tags.HighwayTag;
import org.openstreetmap.atlas.utilities.collections.Iterables;

/**
 * Test to make sure that the {@link ChangeFilter} works properly
 * 
 * @author matthieun
 */
public class AtlasMutatorFilterIntegrationTest extends AbstractAtlasMutatorIntegrationTest
{
    public static final String Z_10_X_499_Y_355 = "/XYZ/XYZ_10-499-355.atlas";
    public static final String Z_11_X_998_Y_708 = "/XYZ/XYZ_11-998-708.atlas";
    public static final String Z_11_X_998_Y_709 = "/XYZ/XYZ_11-998-709.atlas";
    public static final String Z_11_X_999_Y_709 = "/XYZ/XYZ_11-999-709.atlas";

    public static final String APPLIED_0_Z_11_X_998_Y_709 = "/logs/XYZ-0-applied/XYZ-0-applied_11-998-709.geojson.gz";
    public static final String GENERATED_0_Z_10_X_499_Y_355 = "/logs/XYZ-0-generated/XYZ-0-generated_10-499-355.geojson.gz";

    public static final String ASSIGNED_1_Z_11_X_998_Y_709 = "/logs/XYZ-1-assigned/XYZ-1-assigned_11-998-709.geojson.gz";
    public static final String APPLIED_1_Z_11_X_998_Y_709 = "/logs/XYZ-1-applied/XYZ-1-applied_11-998-709.geojson.gz";

    @Rule
    public final AtlasMutatorFilterIntegrationTestRule rule = new AtlasMutatorFilterIntegrationTestRule();

    @Override
    public List<String> arguments()
    {
        final List<String> arguments = new ArrayList<>();
        arguments.add("-" + AtlasMutatorParameters.SUBMISSION_STAGGERING_WAIT.getName() + "=0.01");
        arguments.add("-" + AtlasGeneratorParameters.COUNTRIES.getName() + "=XYZ");
        return arguments;
    }

    @Before
    public void before()
    {
        setup("AtlasMutatorFilterIntegrationTest.json", "tree-6-14-100000.txt",
                "AtlasMutatorFilterIntegrationTest_XYZ.txt");

        // Add the Atlas files to the resource file system
        final long size = 8192;
        final ByteArrayResource resourceZ10X499Y355 = new ByteArrayResource(size);
        final Atlas shardZ10X499Y355 = this.rule.getZ10X499Y355();
        shardZ10X499Y355.save(resourceZ10X499Y355);
        ResourceFileSystem.addResource(INPUT + Z_10_X_499_Y_355, resourceZ10X499Y355);

        final ByteArrayResource resourceZ11X998Y708 = new ByteArrayResource(size);
        final Atlas shardZ11X998Y708 = this.rule.getZ11X998Y708();
        shardZ11X998Y708.save(resourceZ11X998Y708);
        ResourceFileSystem.addResource(INPUT + Z_11_X_998_Y_708, resourceZ11X998Y708);

        final ByteArrayResource resourceZ11X998Y709 = new ByteArrayResource(size);
        final Atlas shardZ11X998Y709 = this.rule.getZ11X998Y709();
        shardZ11X998Y709.save(resourceZ11X998Y709);
        ResourceFileSystem.addResource(INPUT + Z_11_X_998_Y_709, resourceZ11X998Y709);

        final ByteArrayResource resourceZ11X999Y709 = new ByteArrayResource(size);
        final Atlas shardZ11X999Y709 = this.rule.getZ11X999Y709();
        shardZ11X999Y709.save(resourceZ11X999Y709);
        ResourceFileSystem.addResource(INPUT + Z_11_X_999_Y_709, resourceZ11X999Y709);
    }

    @Test
    public void mutate()
    {
        runWithoutQuitting();

        dump();

        final Atlas atlasZ11X998Y709 = atlasForPath(OUTPUT + Z_11_X_998_Y_709);
        Assert.assertEquals(3, atlasZ11X998Y709.numberOfEdges());
        final Iterable<Edge> trunkIterable = atlasZ11X998Y709
                .edgesIntersecting(Polygon.wkt("POLYGON((-4.5553134015972585 48.35014825704579,"
                        + "-4.540893845933196 48.35014825704579,"
                        + "-4.540893845933196 48.33622833589797,"
                        + "-4.5553134015972585 48.33622833589797,"
                        + "-4.5553134015972585 48.35014825704579))"));
        Assert.assertEquals(1, Iterables.size(trunkIterable));
        Assert.assertEquals(HighwayTag.TRUNK, HighwayTag.highwayTag(trunkIterable.iterator().next())
                .orElseThrow(() -> new CoreException("Trunk should be there!")));
        Assert.assertEquals(5, atlasZ11X998Y709.numberOfNodes());
        Assert.assertEquals(0, atlasZ11X998Y709.numberOfAreas());
        // Make sure the residential road in the middle is not included.
        Assert.assertEquals(0,
                Iterables.size(atlasZ11X998Y709.edgesIntersecting(
                        Polygon.wkt("POLYGON((-4.5113680890972585 48.323103618130396,"
                                + "-4.469826035874602 48.323103618130396,"
                                + "-4.469826035874602 48.306093525289086,"
                                + "-4.5113680890972585 48.306093525289086,"
                                + "-4.5113680890972585 48.323103618130396))"))));

        // Make sure the trunk REMOVE geojson log is in the generated list from neighboring shard
        final Resource generated0Z10X499Y355 = forPath(OUTPUT + GENERATED_0_Z_10_X_499_Y_355);
        Assert.assertTrue(Iterables.asList(generated0Z10X499Y355.lines()).stream()
                .anyMatch(assignedGeoJson -> assignedGeoJson.contains(ChangeType.REMOVE.name())
                        && assignedGeoJson.contains("\"identifier\":176767000000")));

        // Make sure the trunk REMOVE geojson log is not in the applied list
        final Resource applied0Z11X998Y709 = forPath(OUTPUT + APPLIED_0_Z_11_X_998_Y_709);
        for (final String appliedGeoJson : applied0Z11X998Y709.lines())
        {
            if (appliedGeoJson.contains(ChangeType.REMOVE.name()))
            {
                Assert.assertFalse("Has unexpected ID: " + appliedGeoJson, appliedGeoJson
                        .contains(/* ID of the trunk road */"\"identifier\":176767000000"));
            }
        }

        // Level 1: Make sure the extraneous bus node is assigned but not applied
        final Resource assigned1Z11X998Y709 = forPath(OUTPUT + ASSIGNED_1_Z_11_X_998_Y_709);
        Assert.assertEquals(1, Iterables.size(assigned1Z11X998Y709.lines()));
        Assert.assertTrue(assigned1Z11X998Y709.all()
                .contains("\"metadata\":{\"mutator\":\"AddTagToBusStops:XYZ_10-499-355\"}"));

        Assert.assertFalse(resourceExits(OUTPUT + APPLIED_1_Z_11_X_998_Y_709));
    }
}
