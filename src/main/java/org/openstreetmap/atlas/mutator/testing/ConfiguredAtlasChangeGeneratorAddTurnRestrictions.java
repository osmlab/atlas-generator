package org.openstreetmap.atlas.mutator.testing;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.builder.RelationBean;
import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.change.testing.AtlasChangeGeneratorAddTurnRestrictions;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEdge;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteNode;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteRelation;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.geography.atlas.items.ItemType;
import org.openstreetmap.atlas.geography.atlas.items.Node;
import org.openstreetmap.atlas.geography.atlas.items.Relation;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;
import org.openstreetmap.atlas.utilities.identifiers.EntityIdentifierGenerator;

import com.google.common.collect.Lists;

/**
 * @author matthieun
 */
public class ConfiguredAtlasChangeGeneratorAddTurnRestrictions
        extends ConfiguredAtlasChangeGenerator
{
    private static final long serialVersionUID = -7944726872773507489L;

    private final AtlasChangeGenerator atlasChangeGeneratorAddTurnRestrictions;
    private final int minimumNodeValence;

    public ConfiguredAtlasChangeGeneratorAddTurnRestrictions(final String name,
            final Configuration configuration)
    {
        super(name, configuration);
        final ConfigurationReader reader = new ConfigurationReader(name);
        final int valence = Integer
                .parseInt(reader.configurationValue(configuration, "node.valence", "3"));
        this.minimumNodeValence = valence;
        this.atlasChangeGeneratorAddTurnRestrictions = new AtlasChangeGeneratorAddTurnRestrictions(
                valence);
    }

    @Override
    public boolean equals(final Object other)
    {
        return super.equals(other);
    }

    // @Override
    // public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    // {
    // return this.atlasChangeGeneratorAddTurnRestrictions.generateWithoutValidation(atlas);
    // }
    // TODO Fix in Atlas and then remove this here. The fix is to use the identifier generator!
    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        final EntityIdentifierGenerator identifierGenerator = new EntityIdentifierGenerator();
        final Set<FeatureChange> result = new HashSet<>();
        final Long parentRelationIdentifier = 999L;
        final RelationBean parentMembers = new RelationBean();
        Rectangle parentBounds = null;
        for (final Node node : atlas.nodes(node -> node.valence() > this.minimumNodeValence))
        {
            final SortedSet<Edge> inEdges = node.inEdges();
            final SortedSet<Edge> outEdges = node.outEdges();
            for (final Edge inEdge : inEdges)
            {
                for (final Edge outEdge : outEdges)
                {
                    final RelationBean members = new RelationBean();
                    members.addItem(inEdge.getIdentifier(), "from", ItemType.EDGE);
                    inEdge.reversed().ifPresent(reversed -> members
                            .addItem(reversed.getIdentifier(), "from", ItemType.EDGE));
                    members.addItem(node.getIdentifier(), "via", ItemType.NODE);
                    members.addItem(outEdge.getIdentifier(), "to", ItemType.EDGE);
                    outEdge.reversed().ifPresent(reversed -> members
                            .addItem(reversed.getIdentifier(), "to", ItemType.EDGE));
                    final Rectangle bounds = Rectangle.forLocated(inEdge, outEdge);
                    if (parentBounds == null)
                    {
                        parentBounds = bounds;
                    }
                    else
                    {
                        parentBounds = Rectangle.forLocated(parentBounds, bounds);
                    }
                    final CompleteRelation completeRelation = new CompleteRelation(0L,
                            Maps.hashMap("type", "restriction", "restriction", "no_left_turn"),
                            bounds, members, Lists.newArrayList(0L), members, 0L,
                            Sets.hashSet(parentRelationIdentifier));
                    final Long relationIdentifier = identifierGenerator
                            .generateIdentifier(completeRelation);
                    parentMembers.addItem(relationIdentifier, "addition", ItemType.RELATION);
                    result.add(FeatureChange.add(new CompleteRelation(relationIdentifier,
                            Maps.hashMap("type", "restriction", "restriction", "no_left_turn"),
                            bounds, members, Lists.newArrayList(relationIdentifier), members,
                            relationIdentifier, Sets.hashSet(parentRelationIdentifier))));
                    result.add(FeatureChange
                            .add(CompleteEdge.shallowFrom(inEdge).withRelationIdentifiers(
                                    mergeRelationMembers(inEdge.relations(), relationIdentifier))));
                    if (inEdge.hasReverseEdge())
                    {
                        result.add(
                                FeatureChange.add(CompleteEdge.shallowFrom(inEdge.reversed().get())
                                        .withRelationIdentifiers(mergeRelationMembers(
                                                inEdge.relations(), relationIdentifier))));
                    }
                    result.add(FeatureChange
                            .add(CompleteNode.shallowFrom(node).withRelationIdentifiers(
                                    mergeRelationMembers(node.relations(), relationIdentifier))));
                    result.add(FeatureChange.add(CompleteEdge.shallowFrom(outEdge)
                            .withRelationIdentifiers(mergeRelationMembers(outEdge.relations(),
                                    relationIdentifier))));
                    if (outEdge.hasReverseEdge())
                    {
                        result.add(
                                FeatureChange.add(CompleteEdge.shallowFrom(outEdge.reversed().get())
                                        .withRelationIdentifiers(mergeRelationMembers(
                                                outEdge.relations(), relationIdentifier))));
                    }
                    // Break here to avoid too many Relation FeatureChanges and make validation
                    // super slow for unit tests.
                    break;
                }
                // Break here to avoid too many Relation FeatureChanges and make validation super
                // slow for unit tests.
                break;
            }
        }
        if (!result.isEmpty())
        {
            result.add(FeatureChange.add(new CompleteRelation(parentRelationIdentifier,
                    Maps.hashMap("name", "parent_of_new_restrictions"), parentBounds, parentMembers,
                    Lists.newArrayList(parentRelationIdentifier), parentMembers,
                    parentRelationIdentifier, Sets.hashSet())));
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    private Set<Long> mergeRelationMembers(final Set<Relation> relations, final Long newIdentifier)
    {
        return Sets.withSets(
                relations.stream().map(Relation::getIdentifier).collect(Collectors.toSet()),
                Sets.hashSet(newIdentifier));
    }
}
