package org.openstreetmap.atlas.mutator;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author lcram
 */
public class MergeForgivenessPolicyIntegrationTestRule extends CoreTestRule
{
    private static final String IMPOSSIBLE = "impossible";

    @TestAtlas(loadFromJosmOsmResource = "merge.josm.osm")
    private Atlas atlas;

    public Atlas getMain()
    {
        return this.atlas;
    }

    /**
     * @return Shard1
     */
    public Atlas getZ6X32Y31()
    {
        return this.atlas.subAtlas(SlippyTile.forName("6-32-31").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return Shard2
     */
    public Atlas getZ7X66Y62()
    {
        return this.atlas.subAtlas(SlippyTile.forName("7-66-62").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return Shard3
     */
    public Atlas getZ7X66Y63()
    {
        return this.atlas.subAtlas(SlippyTile.forName("7-66-63").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }
}
