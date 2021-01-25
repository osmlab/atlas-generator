package org.openstreetmap.atlas.mutator.configuration.mutators.aql;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;

/**
 * @author Yazad Khambata
 */
public class AqlChangeGeneratorTestRule extends CoreTestRule
{
    private static final String ONE = "0,0";
    private static final String TWO = "35,120";

    @SuppressWarnings("unused")
    @TestAtlas(nodes = {
            @TestAtlas.Node(id = "1", coordinates = @TestAtlas.Loc(value = ONE), tags = {
                    "obliviate=memory", "shapeshift=nagini", "country=HWT" }),
            @TestAtlas.Node(id = "2", coordinates = @TestAtlas.Loc(value = TWO), tags = {
                    "author=jk", "country=HWT" }) })
    private Atlas atlas;

    public Atlas getAtlas()
    {
        return this.atlas;
    }
}
