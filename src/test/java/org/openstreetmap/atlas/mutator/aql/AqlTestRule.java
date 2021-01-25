package org.openstreetmap.atlas.mutator.aql;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author Yazad Khambata
 */
public class AqlTestRule extends CoreTestRule
{
    @TestAtlas(loadFromOsmResource = "SanJoseCityHall.osm")
    private Atlas atlas;

    public Atlas getAtlas()
    {
        return this.atlas;
    }
}
