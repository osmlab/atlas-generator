package org.openstreetmap.atlas.mutator.filtering;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.utilities.testing.CoreTestRule;
import org.openstreetmap.atlas.utilities.testing.TestAtlas;
import org.openstreetmap.atlas.utilities.testing.TestAtlas.Edge;
import org.openstreetmap.atlas.utilities.testing.TestAtlas.Loc;
import org.openstreetmap.atlas.utilities.testing.TestAtlas.Node;

/**
 * @author matthieun
 */
public class ChangeFilterTestRule extends CoreTestRule
{
    private static final String NODE_1 = "46.1311895, 7.1065841";
    private static final String NODE_2 = "46.1840544, 7.1830728";

    @TestAtlas(

            nodes = {

                    @Node(coordinates = @Loc(NODE_1)), @Node(coordinates = @Loc(NODE_2))

            },

            edges = {

                    @Edge(coordinates = { @Loc(NODE_1), @Loc(NODE_2) }, tags = { "highway=motorway",
                            "oneway=yes" })

            }

    )
    private Atlas testFilteringExtraneousNodeWithExtraneousEdgeAttachedAtlas;

    public Atlas getTestFilteringExtraneousNodeWithExtraneousEdgeAttachedAtlas()
    {
        return this.testFilteringExtraneousNodeWithExtraneousEdgeAttachedAtlas;
    }
}
