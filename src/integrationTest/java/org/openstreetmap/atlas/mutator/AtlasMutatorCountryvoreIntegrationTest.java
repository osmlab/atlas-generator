package org.openstreetmap.atlas.mutator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.geography.atlas.Atlas;

/**
 * @author matthieun
 */
public class AtlasMutatorCountryvoreIntegrationTest extends AbstractAtlasMutatorIntegrationTest
{
    public static final String XYZ = "XYZ";
    public static final String ABC = "ABC";

    public static final String Z_10_X_507_Y_343 = "10-507-343.atlas";
    public static final String Z_11_X_1016_Y_687 = "11-1016-687.atlas";
    public static final String Z_8_X_126_Y_86 = "8-126-86.atlas";
    public static final String Z_8_X_127_Y_86 = "8-127-86.atlas";

    @Rule
    public final AtlasMutatorCountryvoreIntegrationTestRule rule = new AtlasMutatorCountryvoreIntegrationTestRule();

    @Override
    public List<String> arguments()
    {
        final List<String> arguments = new ArrayList<>();
        arguments.add("-" + AtlasMutatorParameters.SUBMISSION_STAGGERING_WAIT.getName() + "=0.01");
        arguments.add("-" + AtlasGeneratorParameters.COUNTRIES.getName() + "=XYZ,ABC");
        return arguments;
    }

    @Before
    public void before()
    {
        setup("AtlasMutatorCountryvoreIntegrationTest.json", "tree-6-14-100000.txt",
                "AtlasMutatorCountryvoreIntegrationTest_XYZ_ABC.txt");

        // 2 Shards for XYZ
        addAtlasResource(INPUT + "/" + XYZ + "/" + XYZ + "_" + Z_10_X_507_Y_343,
                this.rule.getZ10X507Y343());
        addAtlasResource(INPUT + "/" + XYZ + "/" + XYZ + "_" + Z_11_X_1016_Y_687,
                this.rule.getZ11X1016Y687());

        // 2 Shards for ABC
        addAtlasResource(INPUT + "/" + ABC + "/" + ABC + "_" + Z_8_X_126_Y_86,
                this.rule.getZ8X126Y86());
        addAtlasResource(INPUT + "/" + ABC + "/" + ABC + "_" + Z_8_X_127_Y_86,
                this.rule.getZ8X127Y86());
    }

    @Test
    public void mutate()
    {
        try
        {
            runWithoutQuitting();

            final String expectedArea = "483.4 m^2";

            final Atlas atlasXYZZ10X507Y343 = atlasForPath(
                    OUTPUT + "/" + XYZ + "/" + XYZ + "_" + Z_10_X_507_Y_343);
            atlasXYZZ10X507Y343.lines().forEach(line -> Assert.assertEquals(expectedArea,
                    line.tag(AtlasMutatorCountryvoreIntegrationTestAddMultiPolygonArea.AREA_KEY)));

            final Atlas atlasXYZZ11X1016Y687 = atlasForPath(
                    OUTPUT + "/" + XYZ + "/" + XYZ + "_" + Z_11_X_1016_Y_687);
            atlasXYZZ11X1016Y687.lines().forEach(line -> Assert.assertEquals(expectedArea,
                    line.tag(AtlasMutatorCountryvoreIntegrationTestAddMultiPolygonArea.AREA_KEY)));

            final Atlas atlasABCZ8X126Y86 = atlasForPath(
                    OUTPUT + "/" + ABC + "/" + ABC + "_" + Z_8_X_126_Y_86);
            atlasABCZ8X126Y86.lines().forEach(line -> Assert.assertEquals(expectedArea,
                    line.tag(AtlasMutatorCountryvoreIntegrationTestAddMultiPolygonArea.AREA_KEY)));

            final Atlas atlasABCZ8X127Y86 = atlasForPath(
                    OUTPUT + "/" + ABC + "/" + ABC + "_" + Z_8_X_127_Y_86);
            atlasABCZ8X127Y86.lines().forEach(line -> Assert.assertEquals(expectedArea,
                    line.tag(AtlasMutatorCountryvoreIntegrationTestAddMultiPolygonArea.AREA_KEY)));
        }
        finally
        {
            dump();
        }
    }
}
