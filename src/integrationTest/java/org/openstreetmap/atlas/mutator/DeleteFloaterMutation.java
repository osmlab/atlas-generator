package org.openstreetmap.atlas.mutator;

import java.util.HashSet;
import java.util.Set;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEdge;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteNode;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * Mutation used by {@link AtlasMutatorFilterIntegrationTest}
 *
 * @author matthieun
 */
public class DeleteFloaterMutation extends ConfiguredAtlasChangeGenerator
{
    private static final long serialVersionUID = -3558376594243640902L;

    public DeleteFloaterMutation(final String name, final Configuration configuration)
    {
        super(name, configuration);
    }

    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        final Set<FeatureChange> result = new HashSet<>();
        for (final Edge edge : atlas.edges())
        {
            if (edge.connectedEdges().isEmpty())
            {
                result.add(FeatureChange.remove(CompleteEdge.shallowFrom(edge)));
                result.add(FeatureChange.remove(CompleteNode.shallowFrom(edge.start())));
                result.add(FeatureChange.remove(CompleteNode.shallowFrom(edge.end())));
            }
        }
        return result;
    }
}
