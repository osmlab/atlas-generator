package org.openstreetmap.atlas.generator.slicing;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author jamesgage
 */
public class DynamicRelationSlicingTestRule extends CoreTestRule
{
    @TestAtlas(loadFromJosmOsmResource = "waterbody.josm.osm")
    private Atlas waterBodyAtlas;

    public Atlas getAtlas()
    {
        return this.waterBodyAtlas;
    }
}
