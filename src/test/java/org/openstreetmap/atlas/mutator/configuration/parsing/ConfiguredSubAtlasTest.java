package org.openstreetmap.atlas.mutator.configuration.parsing;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author matthieun
 */
public class ConfiguredSubAtlasTest
{
    /**
     * @author matthieun
     */
    public static class ConfiguredSubAtlasTestRule extends CoreTestRule
    {
        @TestAtlas(loadFromJosmOsmResource = "ConfiguredSubAtlasTest.josm.osm")
        private Atlas atlas;

        public Atlas getAtlas()
        {
            return this.atlas;
        }
    }

    @Rule
    public final ConfiguredSubAtlasTestRule rule = new ConfiguredSubAtlasTestRule();

    @Test
    public void testFilter()
    {
        final Configuration configuration = new StandardConfiguration(
                new InputStreamResource(() -> ConfiguredSubAtlasTest.class.getResourceAsStream(
                        ConfiguredSubAtlasTest.class.getSimpleName() + ".json")));
        final Atlas atlas = this.rule.getAtlas();
        final ConfiguredSubAtlas junctionRoundaboutSoftCut = ConfiguredSubAtlas
                .from("junctionRoundaboutSoftCutSubAtlas", configuration);
        final Atlas junctionRoundaboutSoftCutSubAtlas = junctionRoundaboutSoftCut.apply(atlas)
                .orElseThrow(
                        () -> new CoreException("junctionRoundaboutSoftCutSubAtlas is empty!"));
        Assert.assertEquals(1, junctionRoundaboutSoftCutSubAtlas.numberOfEdges());

        final ConfiguredSubAtlas notThere = ConfiguredSubAtlas.from("notThere", configuration);
        final Atlas notThereSubAtlas = notThere.apply(atlas)
                .orElseThrow(() -> new CoreException("notThere is empty!"));
        Assert.assertEquals(4, notThereSubAtlas.numberOfEdges());
    }
}
