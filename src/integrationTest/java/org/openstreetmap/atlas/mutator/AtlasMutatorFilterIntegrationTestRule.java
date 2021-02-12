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
public class AtlasMutatorFilterIntegrationTestRule extends CoreTestRule
{
    private static final String IMPOSSIBLE = "impossible";

    @TestAtlas(loadFromJosmOsmResource = "AtlasMutatorFilterIntegrationTest.josm.osm")
    private Atlas source;

    public Atlas getSource()
    {
        return this.source;
    }

    /**
     * @return 10-499-355
     */
    public Atlas getZ10X499Y355()
    {
        return this.source
                .subAtlas(SlippyTile.forName("10-499-355").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return 11-998-708
     */
    public Atlas getZ11X998Y708()
    {
        return this.source
                .subAtlas(SlippyTile.forName("11-998-708").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return 11-998-709
     */
    public Atlas getZ11X998Y709()
    {
        return this.source
                .subAtlas(SlippyTile.forName("11-998-709").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }

    /**
     * @return 11-999-709
     */
    public Atlas getZ11X999Y709()
    {
        return this.source
                .subAtlas(SlippyTile.forName("11-999-709").bounds(), AtlasCutType.SOFT_CUT)
                .orElseThrow(() -> new CoreException(IMPOSSIBLE));
    }
}
