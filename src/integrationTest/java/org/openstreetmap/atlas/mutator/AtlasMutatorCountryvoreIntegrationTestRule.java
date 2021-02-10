package org.openstreetmap.atlas.mutator;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author matthieun
 */
public class AtlasMutatorCountryvoreIntegrationTestRule extends CoreTestRule
{
    private static final String IMPOSSIBLE = "impossible";

    @TestAtlas(loadFromJosmOsmResource = "AtlasMutatorCountryvoreIntegrationTest.josm.osm")
    private Atlas source;

    public Atlas getSource()
    {
        return this.source;
    }

    /**
     * @return 10-507-343
     */
    public Atlas getZ10X507Y343()
    {
        return this.source
                .subAtlas(SlippyTile.forName("10-507-343").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return 11-1016-687
     */
    public Atlas getZ11X1016Y687()
    {
        return this.source
                .subAtlas(SlippyTile.forName("11-1016-687").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return 8-126-86
     */
    public Atlas getZ8X126Y86()
    {
        return this.source.subAtlas(SlippyTile.forName("8-126-86").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return 8-127-86
     */
    public Atlas getZ8X127Y86()
    {
        return this.source.subAtlas(SlippyTile.forName("8-127-86").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }
}
