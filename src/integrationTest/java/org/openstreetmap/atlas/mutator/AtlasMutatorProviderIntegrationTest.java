package org.openstreetmap.atlas.mutator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.testing.OsmFileParser;
import org.openstreetmap.atlas.utilities.testing.OsmFileToPbf;

/**
 * @author matthieun
 */
public class AtlasMutatorProviderIntegrationTest extends AbstractAtlasMutatorIntegrationTest
{
    public static final String PBF_Z_11_X_1014_Y_748 = "/11/1014/748/11-1014-748.pbf";
    public static final String PBF_Z_11_X_1015_Y_748 = "/11/1015/748/11-1015-748.pbf";

    public static final String Z_11_X_1014_Y_748 = "/XYZ/XYZ_11-1014-748.atlas";
    public static final String Z_11_X_1015_Y_748 = "/XYZ/XYZ_11-1015-748.atlas";

    public static final String DEFAULT_TAG_KEY = "someKey";
    public static final String DEFAULT_TAG_VALUE = "someValue";

    @Override
    public List<String> arguments()
    {
        final List<String> arguments = new ArrayList<>();
        arguments.add("-" + AtlasGeneratorParameters.COUNTRIES.getName() + "=XYZ");
        return arguments;
    }

    @Before
    public void before()
    {
        setup("AtlasMutatorProviderIntegrationTest.json", "tree-6-14-100000.txt",
                "AtlasMutatorProviderIntegrationTest_XYZ.txt");

        // Add the PBF files to the resource file system
        final Resource pbfZ11X1014Y748 = convertToPbf(new InputStreamResource(
                () -> AtlasMutatorProviderIntegrationTest.class.getResourceAsStream(
                        "AtlasMutatorProviderIntegrationTest-11-1014-748.josm.osm")));
        addResource(INPUT + PBF_Z_11_X_1014_Y_748, pbfZ11X1014Y748);

        final Resource pbfZ11X1015Y748 = convertToPbf(new InputStreamResource(
                () -> AtlasMutatorProviderIntegrationTest.class.getResourceAsStream(
                        "AtlasMutatorProviderIntegrationTest-11-1015-748.josm.osm")));
        addResource(INPUT + PBF_Z_11_X_1015_Y_748, pbfZ11X1015Y748);
    }

    @Test
    public void mutate()
    {
        runWithoutQuitting();

        dump();

        final Atlas atlasZ11X1014Y748 = atlasForPath(OUTPUT + Z_11_X_1014_Y_748);
        final Atlas atlasZ11X1015Y748 = atlasForPath(OUTPUT + Z_11_X_1015_Y_748);

        Assert.assertEquals(1, atlasZ11X1014Y748.numberOfLines());
        Assert.assertEquals(2, atlasZ11X1014Y748.numberOfPoints());
        atlasZ11X1014Y748.entities()
                .forEach(entity -> Assert.assertEquals(
                        "Entity did not have the right tag: " + entity.toString(),
                        DEFAULT_TAG_VALUE, entity.tag(DEFAULT_TAG_KEY)));

        Assert.assertEquals(2, atlasZ11X1015Y748.numberOfLines());
        Assert.assertEquals(7, atlasZ11X1015Y748.numberOfPoints());
        atlasZ11X1015Y748.entities()
                .forEach(entity -> Assert.assertEquals(
                        "Entity did not have the right tag: " + entity.toString(),
                        DEFAULT_TAG_VALUE, entity.tag(DEFAULT_TAG_KEY)));
    }

    private Resource convertToPbf(final Resource josmOsmResource)
    {
        final ByteArrayResource pbfFile = new ByteArrayResource();
        final StringResource osmFile = new StringResource();
        new OsmFileParser().update(josmOsmResource, osmFile);
        new OsmFileToPbf().update(osmFile, pbfFile);
        return pbfFile;
    }
}
