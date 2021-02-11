package org.openstreetmap.atlas.mutator;

import java.util.HashSet;
import java.util.Set;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteNode;
import org.openstreetmap.atlas.geography.atlas.items.Node;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.tags.HighwayTag;
import org.openstreetmap.atlas.tags.annotations.validation.Validators;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * Mutation used by {@link AtlasMutatorFilterIntegrationTest}
 *
 * @author matthieun
 */
public class AddTagToBusStopsMutation extends ConfiguredAtlasChangeGenerator
{
    private static final long serialVersionUID = -3558376594243640902L;

    public AddTagToBusStopsMutation(final String name, final Configuration configuration)
    {
        super(name, configuration);
    }

    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        final Set<FeatureChange> result = new HashSet<>();
        for (final Node node : atlas.nodes())
        {
            if (Validators.isOfType(node, HighwayTag.class, HighwayTag.BUS_STOP))
            {
                // Here use "from" and not "shallowFrom" since there is a filter getting rid of
                // shallow changes before the `shouldKeepExtraneousNodeUpdate` method we want to
                // test
                result.add(
                        FeatureChange.add(CompleteNode.from(node).withAddedTag("my:new", "tag")));
            }
        }
        return result;
    }
}
