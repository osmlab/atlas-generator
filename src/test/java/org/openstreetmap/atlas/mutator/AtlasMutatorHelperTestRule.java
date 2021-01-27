package org.openstreetmap.atlas.mutator;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author matthieun
 */
public class AtlasMutatorHelperTestRule extends CoreTestRule
{
    @TestAtlas(loadFromJosmOsmResource = "changeToAtlas.josm.osm")
    private Atlas changeToAtlas;

    @TestAtlas(loadFromJosmOsmResource = "changeToAtlasEdge.josm.osm")
    private Atlas changeToAtlasEdge;

    @TestAtlas(loadFromJosmOsmResource = "changeToAtlasRelation.josm.osm")
    private Atlas changeToAtlasRelation;

    public Atlas getChangeToAtlas()
    {
        return this.changeToAtlas;
    }

    public Atlas getChangeToAtlasEdge()
    {
        return this.changeToAtlasEdge;
    }

    public Atlas getChangeToAtlasRelation()
    {
        return this.changeToAtlasRelation;
    }
}
