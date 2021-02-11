package org.openstreetmap.atlas.mutator.configuration.parsing;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author matthieun
 */
public class ConfiguredDynamicAtlasPolicyTestRule extends CoreTestRule
{
    private static final String IMPOSSIBLE = "impossible";

    @TestAtlas(loadFromJosmOsmResource = "ConfiguredDynamicAtlasPolicyTest.josm.osm")
    private Atlas atlas;

    /**
     * @return Shard2
     */
    public Atlas getZ8X131Y97()
    {
        return this.atlas.subAtlas(SlippyTile.forName("8-131-97").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return Shard3
     */
    public Atlas getZ8X131Y98()
    {
        return this.atlas.subAtlas(SlippyTile.forName("8-131-98").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return Shard1
     */
    public Atlas getZ9X261Y195()
    {
        return this.atlas.subAtlas(SlippyTile.forName("9-261-195").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }
}
