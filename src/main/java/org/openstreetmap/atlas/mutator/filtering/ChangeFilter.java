package org.openstreetmap.atlas.mutator.filtering;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.PolyLine;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.change.AtlasEntityKey;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.ChangeBuilder;
import org.openstreetmap.atlas.geography.atlas.change.ChangeType;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChangeBoundsExpander;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEntity;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteNode;
import org.openstreetmap.atlas.geography.atlas.dynamic.DynamicAtlas;
import org.openstreetmap.atlas.geography.atlas.items.Area;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.geography.atlas.items.ItemType;
import org.openstreetmap.atlas.geography.atlas.items.LineItem;
import org.openstreetmap.atlas.geography.atlas.items.LocationItem;
import org.openstreetmap.atlas.geography.atlas.items.Node;
import org.openstreetmap.atlas.geography.atlas.items.Relation;
import org.openstreetmap.atlas.geography.atlas.items.RelationMember;
import org.openstreetmap.atlas.geography.atlas.items.RelationMemberList;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.mutator.AtlasMutator;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.MultiIterable;
import org.openstreetmap.atlas.utilities.tuples.Tuple;

/**
 * Filter out all FeatureChange objects that do not belong to the specified DynamicAtlas (or initial
 * shard).
 *
 * @author matthieun
 */
public class ChangeFilter implements Function<Change, Optional<Change>>
{
    private static final Predicate<Map.Entry<String, String>> MUTATOR_TAG_PREDICATE = entry -> entry
            .getKey() != null
            && entry.getKey().startsWith(
                    AtlasMutator.MUTATOR_META_DATA_KEY + AtlasMutator.MUTATOR_META_DATA_SPLIT);
    private final DynamicAtlas atlas;
    private final Shard shard;

    public static Change changeWithoutMutatorTag(final Change input,
            final Predicate<FeatureChange> entitiesToConsider)
    {
        final ChangeBuilder result = new ChangeBuilder();
        input.changes().map(
                featureChange -> featureChangeWithoutMutatorTag(featureChange, entitiesToConsider))
                .forEach(result::add);
        return result.get();
    }

    /**
     * Get a map of meta-data tags to add to an Atlas
     *
     * @param change
     *            The change object being applied
     * @param entitiesToConsider
     *            A predicate choosing only the entities of interest
     * @return A map of meta-data tags to add to an Atlas
     */
    public static Map<String, String> mutatorMetaDataFromTags(final Change change,
            final Predicate<FeatureChange> entitiesToConsider)
    {
        return change.changes().filter(entitiesToConsider)
                .filter(featureChange -> featureChange.getTags() != null)
                .flatMap(featureChange -> featureChange.getTags().entrySet().stream()
                        .filter(MUTATOR_TAG_PREDICATE)
                        .map(entry -> new Tuple<>(
                                entry.getKey() + AtlasMutator.MUTATOR_META_DATA_SPLIT
                                        + featureChange.getItemType().name()
                                        + AtlasMutator.MUTATOR_META_DATA_SPLIT
                                        + featureChange.getIdentifier(),
                                entry.getValue())))
                .collect(Collectors.toMap(Tuple::getFirst, Tuple::getSecond));
    }

    public static Optional<Change> stripForSaving(final Change change)
    {
        // Get rid of all the artificial removes added for shard consistency
        return createChange(change.getFeatureChanges().stream()
                .filter(featureChange -> !featureChange.getMetaData().isEmpty())
                .collect(Collectors.toList()));
    }

    protected static FeatureChange featureChangeWithoutMutatorTag(final FeatureChange input,
            final Predicate<FeatureChange> entitiesToConsider)
    {
        if (ChangeType.ADD == input.getChangeType() && entitiesToConsider.test(input)
                && input.getTags() != null)
        {
            final Set<String> keysToRemove = input.getAfterView().getTags().entrySet().stream()
                    .filter(MUTATOR_TAG_PREDICATE).map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            keysToRemove.forEach(key -> input.getAfterView().getTags().remove(key));
        }
        return input;
    }

    private static Optional<Change> createChange(final Iterable<FeatureChange> featureChanges)
    {
        final ChangeBuilder result = new ChangeBuilder();
        featureChanges.forEach(result::add);
        if (result.peekNumberOfChanges() > 0)
        {
            return Optional.of(result.get());
        }
        else
        {
            return Optional.empty();
        }
    }

    public ChangeFilter(final DynamicAtlas atlas)
    {
        this.atlas = atlas;
        this.shard = null;
    }

    public ChangeFilter(final Shard shard)
    {
        this.atlas = null;
        this.shard = shard;
    }

    @Override
    public Optional<Change> apply(final Change change)
    {
        if (this.atlas != null)
        {
            return stripUnwantedFeatures(change);
        }
        else
        {
            return stripUnwantedFeaturesWithoutAtlas(change);
        }
    }

    /**
     * @param featureChange
     *            A {@link FeatureChange} of type Node
     * @param change
     *            A change with all the updated features, including new oe updated connected nodes.
     * @return A set of all connected edges to the provided node, from the provided change.
     */
    private Set<Edge> connectedEdges(final FeatureChange featureChange, final Change change)
    {
        final Set<Edge> connectedEdges = new HashSet<>();
        final CompleteNode node = (CompleteNode) featureChange.getAfterView();
        final Set<Long> inEdgeIdentifiers = node.inEdgeIdentifiers();
        final Set<Long> outEdgeIdentifiers = node.outEdgeIdentifiers();
        final Set<Long> connectedEdgeIdentifiers = new HashSet<>();
        if (inEdgeIdentifiers != null)
        {
            connectedEdgeIdentifiers.addAll(inEdgeIdentifiers);
        }
        if (outEdgeIdentifiers != null)
        {
            connectedEdgeIdentifiers.addAll(outEdgeIdentifiers);
        }
        connectedEdgeIdentifiers.stream().map(edgeIdentifier ->
        {
            Edge originalEdge = this.atlas.edge(edgeIdentifier);
            if (originalEdge == null)
            {
                final Optional<Edge> edgeOption = change.getFeatureChanges().stream().filter(
                        changeFeatureChange -> changeFeatureChange.getIdentifier() == edgeIdentifier
                                && changeFeatureChange.getItemType() == ItemType.EDGE)
                        .findFirst().map(FeatureChange::getAfterView)
                        .map(atlasEntity -> (Edge) atlasEntity);
                if (edgeOption.isPresent())
                {
                    originalEdge = edgeOption.get();
                }
            }
            return originalEdge;
        }).filter(Objects::nonNull).forEach(connectedEdges::add);
        return connectedEdges;
    }

    /**
     * @param initialShardBounds
     *            The bounds of the initial shards in the DynamicAtlas
     * @return A function which decides for each FeatureChange of type ADD Edge provided whether its
     *         shape actually overlaps the initial bounds. This is to filter out all the instances
     *         where an edge's bounds overlap the shard, when the edge's shape does not.
     */
    private Predicate<FeatureChange> edgeOverlap(final MultiPolygon initialShardBounds) // NOSONAR
    {
        return featureChange ->
        {
            if (ItemType.EDGE == featureChange.getItemType()
                    && ChangeType.ADD == featureChange.getChangeType())
            {
                final List<PolyLine> shapes = new ArrayList<>();
                final Edge afterView = (Edge) featureChange.getAfterView();
                final PolyLine shapeAfter = afterView.asPolyLine();
                if (shapeAfter != null)
                {
                    shapes.add(shapeAfter);
                }
                final Edge beforeView = (Edge) featureChange.getBeforeView();
                if (beforeView != null)
                {
                    final PolyLine shapeBefore = beforeView.asPolyLine();
                    if (shapeBefore != null)
                    {
                        shapes.add(shapeBefore);
                    }
                }
                if (shapes.isEmpty())
                {
                    if (this.atlas == null)
                    {
                        throw new CoreException("There should never be an ADD EDGE FeatureChange"
                                + " with no geometry in before/after views and no pre-existing "
                                + "atlas to reference it:\n{}", featureChange.prettify());
                    }
                    final Edge edgeFromAtlas = this.atlas.edge(featureChange.getIdentifier());
                    if (edgeFromAtlas == null)
                    {
                        throw new CoreException("There should never be an ADD EDGE FeatureChange"
                                + " with no geometry in before/after views and no pre-existing "
                                + "corresponding feature in the Atlas to apply it to:\n{}",
                                featureChange.prettify());
                    }
                    shapes.add(edgeFromAtlas.asPolyLine());
                }
                return shapes.stream().anyMatch(initialShardBounds::overlaps);
            }
            return true;
        };
    }

    private boolean geometricRelationChanges(final FeatureChange featureChange, final Change change)
    {
        if (featureChange.getItemType().equals(ItemType.EDGE)
                && featureChange.getChangeType().equals(ChangeType.ADD)
                && featureChange.getAfterView().relations() != null)
        {
            if (featureChange.getAfterView().relations().stream()
                    .anyMatch(relation -> relation.isGeometric()))
            {
                return true;
            }
        }
        return false;
    }

    private Predicate<FeatureChange> overlap(final MultiPolygon initialShardBounds,
            final Change originalChange)
    {
        return featureChange ->
        {
            if (featureChange.getItemType() == ItemType.EDGE)
            {
                return edgeOverlap(initialShardBounds).test(featureChange);
            }
            if (featureChange.getItemType() == ItemType.RELATION)
            {
                return relationOverlap(initialShardBounds, originalChange).test(featureChange);
            }
            return true;
        };
    }

    private void populateAreas(final long memberIdentifier,
            final Map<AtlasEntityKey, FeatureChange> keyToFeatureChangeMap,
            final Set<Polygon> areas)
    {
        if (this.atlas != null)
        {
            final Area areaFromAtlas = this.atlas.area(memberIdentifier);
            if (areaFromAtlas != null)
            {
                areas.add(areaFromAtlas.asPolygon());
            }
        }
        final AtlasEntityKey atlasEntityKeyArea = AtlasEntityKey.from(ItemType.AREA,
                memberIdentifier);
        if (keyToFeatureChangeMap.containsKey(atlasEntityKeyArea))
        {
            final FeatureChange memberFeatureChange = keyToFeatureChangeMap.get(atlasEntityKeyArea);
            final Polygon areaFromFeatureChange = ((Area) memberFeatureChange.getAfterView())
                    .asPolygon();
            if (areaFromFeatureChange != null)
            {
                areas.add(areaFromFeatureChange);
            }
        }
    }

    private void populateLines(final long memberIdentifier, final ItemType memberType,
            final Map<AtlasEntityKey, FeatureChange> keyToFeatureChangeMap,
            final Set<PolyLine> lines)
    {
        if (this.atlas != null)
        {
            final LineItem lineFromAtlas = (LineItem) this.atlas.entity(memberIdentifier,
                    memberType);
            if (lineFromAtlas != null)
            {
                lines.add(lineFromAtlas.asPolyLine());
            }
        }
        final AtlasEntityKey atlasEntityKeyLine = AtlasEntityKey.from(memberType, memberIdentifier);
        if (keyToFeatureChangeMap.containsKey(atlasEntityKeyLine))
        {
            final FeatureChange memberFeatureChange = keyToFeatureChangeMap.get(atlasEntityKeyLine);
            final PolyLine lineFromFeatureChange = ((LineItem) memberFeatureChange.getAfterView())
                    .asPolyLine();
            if (lineFromFeatureChange != null)
            {
                lines.add(lineFromFeatureChange);
            }
        }
    }

    private void populatePoints(final long memberIdentifier, final ItemType memberType,
            final Map<AtlasEntityKey, FeatureChange> keyToFeatureChangeMap,
            final Set<Location> points)
    {
        if (this.atlas != null)
        {
            final LocationItem pointFromAtlas = (LocationItem) this.atlas.entity(memberIdentifier,
                    memberType);
            if (pointFromAtlas != null)
            {
                points.add(pointFromAtlas.getLocation());
            }
        }
        final AtlasEntityKey atlasEntityKeyPoint = AtlasEntityKey.from(memberType,
                memberIdentifier);
        if (keyToFeatureChangeMap.containsKey(atlasEntityKeyPoint))
        {
            final FeatureChange memberFeatureChange = keyToFeatureChangeMap
                    .get(atlasEntityKeyPoint);
            final Location pointFromFeatureChange = ((LocationItem) memberFeatureChange
                    .getAfterView()).getLocation();
            if (pointFromFeatureChange != null)
            {
                points.add(pointFromFeatureChange);
            }
        }
    }

    private void populateRelationShapes(final Change originalChange, final long relationIdentifier,
            final Set<Polygon> areas, final Set<PolyLine> lines, final Set<Location> points)
    {
        final Map<AtlasEntityKey, FeatureChange> keyToFeatureChangeMap = originalChange
                .allChangesMappedByAtlasEntityKey();
        Relation afterView = null;
        if (keyToFeatureChangeMap
                .containsKey(AtlasEntityKey.from(ItemType.RELATION, relationIdentifier)))
        {
            afterView = (Relation) keyToFeatureChangeMap
                    .get(AtlasEntityKey.from(ItemType.RELATION, relationIdentifier)).getAfterView();
        }
        Relation source = null;
        if (this.atlas != null)
        {
            source = this.atlas.relation(relationIdentifier);
        }
        final List<RelationMember> members = new ArrayList<>();
        if (source != null)
        {
            members.addAll(source.members());
        }
        if (afterView != null)
        {
            final RelationMemberList afterMembers = afterView.members();
            if (afterMembers != null)
            {
                members.addAll(afterMembers);
            }
        }
        if (!members.isEmpty())
        {
            members.forEach(relationMember ->
            {
                final AtlasEntity memberEntity = relationMember.getEntity();
                final ItemType memberType = memberEntity.getType();
                final long memberIdentifier = memberEntity.getIdentifier();
                switch (memberType)
                {
                    case AREA:
                        populateAreas(memberIdentifier, keyToFeatureChangeMap, areas);
                        break;
                    case LINE:
                    case EDGE:
                        populateLines(memberIdentifier, memberType, keyToFeatureChangeMap, lines);
                        break;
                    case NODE:
                    case POINT:
                        populatePoints(memberIdentifier, memberType, keyToFeatureChangeMap, points);
                        break;
                    case RELATION:
                        populateRelationShapes(originalChange, memberIdentifier, areas, lines,
                                points);
                        break;
                    default:
                        throw new CoreException("Unknown type");
                }
            });
        }
    }

    /**
     * @param initialShardBounds
     *            The bounds of the initial shards in the DynamicAtlas
     * @param originalChange
     *            The Change object from which the provided FeatureChanges come from
     * @return A function which decides for each FeatureChange of type ADD RELATION provided whether
     *         its shape actually overlaps the initial bounds. This is to filter out all the
     *         instances where a relation's bounds overlap the shard, when the members' shapes do
     *         not. It needs access to the original change since some members might be new and not
     *         found in the initial Atlas.
     */
    private Predicate<FeatureChange> relationOverlap(final MultiPolygon initialShardBounds,
            final Change originalChange)
    {
        return featureChange ->
        {
            if (ItemType.RELATION == featureChange.getItemType()
                    && ChangeType.ADD == featureChange.getChangeType())
            {
                final Set<Polygon> areas = new HashSet<>();
                final Set<PolyLine> lines = new HashSet<>();
                final Set<Location> points = new HashSet<>();
                populateRelationShapes(originalChange, featureChange.getIdentifier(), areas, lines,
                        points);
                return areas.stream().anyMatch(initialShardBounds::overlaps)
                        || lines.stream().anyMatch(initialShardBounds::overlaps)
                        || points.stream().anyMatch(initialShardBounds::fullyGeometricallyEncloses);
            }
            return true;
        };
    }

    private Predicate<FeatureChange> shouldKeepExtraneousEdgeRemoval(
            final Set<String> initialShardNames)
    {
        return featureChange ->
        {
            if (ItemType.EDGE == featureChange.getItemType()
                    && ChangeType.REMOVE == featureChange.getChangeType()
                    && initialShardNames.size() == 1)
            {
                // This is a flimsy check which will work in the vast majority of cases
                // When applying an Edge REMOVE which is applied here because the relation the Edge
                // is a member of is really large, and the shard generating that change from another
                // shard and not this one, we can say that REMOVE does not belong here.
                final String initialShardName = initialShardNames.iterator().next();
                final String metaDataMutatorSource = featureChange.getMetaData()
                        .get(AtlasMutator.MUTATOR_META_DATA_KEY);
                // Keep the edge removal only when we can verify that it has been generated by at
                // least the same shard. Otherwise we can assume that it is a boundary effect that
                // we want to have ignored.
                return metaDataMutatorSource == null
                        || metaDataMutatorSource.contains(initialShardName);
            }
            return true;
        };
    }

    /**
     * @param initialShardNames
     *            The names of the initial shards in the DynamicAtlas
     * @return A function which decides for each FeatureChange of provided whether its bounds are
     *         eligible to be applied. Since edges that are part of a large relation will have their
     *         bounds inflated by the {@link FeatureChangeBoundsExpander}, some boundary effects
     *         where edge removes are applied from too far away need to be averted. Other cases
     *         where some node updates are coming from far-flung shards because they belong to a
     *         relation that is large, need to be filtered out when we can verify that the current
     *         shard did not generate those updates. This function decides that if a change is
     *         generated by a different shard, and is coming from a shard that is far, it needs to
     *         be excluded from the current shard's Change.
     */
    private Predicate<FeatureChange> shouldKeepExtraneousFeatureChange(final Change change,
            final Set<String> initialShardNames)
    {
        return shouldKeepExtraneousEdgeRemoval(initialShardNames)
                .and(shouldKeepExtraneousNodeUpdate(change, initialShardNames));
    }

    /**
     * Choose nodes to be excluded from the current change. To be excluded, a node FC has to come
     * from another shard only, and none of its connected edges (existing and new) intersect the
     * current shard's bounds.
     *
     * @param change
     *            The current change (to find all the new connected edges)
     * @param initialShardNames
     *            The list of initial shards (to find if the FC originates from one of the current
     *            shards)
     * @return A predicate that returns True id the Node FC should be kept.
     */
    private Predicate<FeatureChange> shouldKeepExtraneousNodeUpdate(final Change change,
            final Set<String> initialShardNames)
    {
        return featureChange ->
        {
            if (ItemType.NODE == featureChange.getItemType()
                    && ChangeType.ADD == featureChange.getChangeType()
                    && initialShardNames.size() == 1)
            {
                final String initialShardName = initialShardNames.iterator().next();
                final String metaDataMutatorSource = featureChange.getMetaData()
                        .get(AtlasMutator.MUTATOR_META_DATA_KEY);
                final Node originalNode = this.atlas.node(featureChange.getIdentifier());
                final Set<Edge> connectedEdges = new HashSet<>();
                if (originalNode != null)
                {
                    connectedEdges.addAll(originalNode.connectedEdges());
                }
                connectedEdges.addAll(connectedEdges(featureChange, change));
                final Rectangle shardBounds = this.atlas.getPolicy().getInitialShards().iterator()
                        .next().bounds();
                return metaDataMutatorSource == null
                        || metaDataMutatorSource.contains(initialShardName)
                        || connectedEdges.stream().filter(edge -> edge.asPolyLine() != null)
                                .anyMatch(edge -> shardBounds.overlaps(edge.asPolyLine()));
            }
            return true;
        };
    }

    private Optional<Change> stripUnwantedFeatures(final Change change)
    {
        final MultiPolygon initialShardBounds = this.atlas.getPolicy().getInitialShardsBounds();
        final Set<String> initialShardNames = this.atlas.getPolicy().getInitialShards().stream()
                .map(Shard::getName).collect(Collectors.toSet());
        final List<FeatureChange> filteredGeometricRelationChanges = change.changes()
                // Strip large relation FCs from other shards
                .filter(featureChange -> geometricRelationChanges(featureChange, change))
                // Collect
                .collect(Collectors.toList());
        final List<FeatureChange> filteredChanges = change.changes()
                // Strip shallow feature changes
                .filter(featureChange -> featureChange.afterViewIsFull() || this.atlas
                        .entity(featureChange.getIdentifier(), featureChange.getItemType()) != null)
                // Strip second degree edges
                .filter(featureChange -> overlap(initialShardBounds, change).test(featureChange))
                .filter(featureChange -> !filteredGeometricRelationChanges.contains(featureChange))
                // Strip large relation FCs from other shards
                .filter(shouldKeepExtraneousFeatureChange(change, initialShardNames))
                // Collect
                .collect(Collectors.toList());

        // Keep track of all the edge identifiers that are being added by the mutation and that are
        // still within the initial bounds.
        final Set<AtlasEntityKey> filteredChangeIdentifiers = filteredChanges.stream()
                .filter(featureChange -> (featureChange.getItemType() == ItemType.EDGE
                        || featureChange.getItemType() == ItemType.RELATION)
                        && featureChange.getChangeType() == ChangeType.ADD)
                .map(featureChange -> AtlasEntityKey.from(featureChange.getItemType(),
                        featureChange.getIdentifier()))
                .collect(Collectors.toSet());
        // Add "remove" changes for all edges completely outside the initial shard bounds, and for
        // which the mutation did not create an ADD that becomes within the bounds.
        final Iterable<FeatureChange> additionalRemoves = Iterables
                .stream(new MultiIterable<>(this.atlas.edges(), this.atlas.relations()))
                .filter(entity -> !filteredChangeIdentifiers
                        .contains(AtlasEntityKey.from(entity.getType(), entity.getIdentifier())))
                .filter(entity -> !filteredGeometricRelationChanges.stream()
                        .anyMatch(featureChange -> featureChange.getItemType().equals(ItemType.EDGE)
                                && featureChange.getIdentifier() == entity.getIdentifier()))
                .filter(entity -> !overlap(initialShardBounds, change)
                        // Here this first FeatureChange is meaningless, just used
                        // to be able to
                        // call the overlaps method on the specific atlas entity.
                        .test(FeatureChange.add(CompleteEntity.from(entity))))
                .map(entity -> FeatureChange.remove(CompleteEntity.shallowFrom(entity),
                        this.atlas));
        final Iterable<FeatureChange> result = new MultiIterable<>(filteredChanges,
                additionalRemoves);
        return createChange(result);
    }

    private Optional<Change> stripUnwantedFeaturesWithoutAtlas(final Change change)
    {
        final MultiPolygon initialShardBounds = MultiPolygon.forPolygon(this.shard.bounds());
        final Iterable<FeatureChange> filteredChanges = change.changes()
                // Strip shallow feature changes
                .filter(FeatureChange::afterViewIsFull)
                // Strip second degree edges
                .filter(overlap(initialShardBounds, change))
                // Collect
                .collect(Collectors.toList());
        return createChange(filteredChanges);
    }
}
