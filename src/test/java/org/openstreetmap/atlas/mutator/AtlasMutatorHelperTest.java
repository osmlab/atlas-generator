package org.openstreetmap.atlas.mutator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.PolyLine;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.ChangeBuilder;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteArea;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEdge;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteLine;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteNode;
import org.openstreetmap.atlas.geography.atlas.complete.CompletePoint;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteRelation;
import org.openstreetmap.atlas.geography.atlas.items.Area;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.DynamicTileSharding;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.mutators.BasicConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.tags.ISOCountryTag;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.identifiers.EntityIdentifierGenerator;
import org.openstreetmap.atlas.utilities.scalars.Distance;
import org.openstreetmap.atlas.utilities.tuples.Tuple;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * @author matthieun
 */
public class AtlasMutatorHelperTest
{
    public static final Sharding SHARDING = new DynamicTileSharding(new InputStreamResource(
            () -> AtlasMutatorHelperTest.class.getResourceAsStream("tree-6-14-100000.txt")));

    private static final String TEST_FAILED = "Test failed";
    private static final String MERGE_FAILED = "Merge failed";
    private static final String MUTATED_KEY = "mutated";
    private static final String MUTATED_VALUE = "yes";
    private static final String SHARD_9_261_195 = "9-261-195";
    private static final String SHARD_8_130_98 = "8-130-98";
    private static final String SHARD_8_131_97 = "8-131-97";
    private static final String SHARD_OUTSIDE = "8-132-99";
    private static final String COUNTRY = "XYZ";

    public static final CountryBoundaryMap BOUNDARIES = CountryBoundaryMap
            .fromPlainText(new InputStreamResource(
                    () -> AtlasMutatorHelperTest.class.getResourceAsStream(COUNTRY + ".txt")));

    private static final String GROUP = "GROUP0";
    private static final CountryShard COUNTRY_SHARD_9_261_195 = new CountryShard(COUNTRY,
            SHARD_9_261_195);
    private static final CountryShard COUNTRY_SHARD_8_130_98 = new CountryShard(COUNTRY,
            SHARD_8_130_98);
    private static final CountryShard COUNTRY_SHARD_8_131_97 = new CountryShard(COUNTRY,
            SHARD_8_131_97);
    private static final CountryShard COUNTRY_SHARD_OUTSIDE = new CountryShard(COUNTRY,
            SHARD_OUTSIDE);
    private static final Map<String, String> FOREST = Maps.hashMap("landuse", "forest");
    private static final String TEST_MUTATOR_NAME = "testMutator";
    private static final Area AREA_123 = new CompleteArea(123L, Polygon.SILICON_VALLEY, FOREST,
            Sets.hashSet());
    private static final Area AREA_456 = new CompleteArea(456L, Polygon.SILICON_VALLEY, FOREST,
            Sets.hashSet());
    private static final Area AREA_789 = new CompleteArea(789L, Polygon.SILICON_VALLEY, FOREST,
            Sets.hashSet());
    private static final Configuration EMPTY = new StandardConfiguration(new StringResource("{}"));
    private static final Configuration NO_EXPANSION_CONFIGURATION = new StandardConfiguration(
            new InputStreamResource(() -> AtlasMutatorHelperTest.class
                    .getResourceAsStream("noExpansionConfiguration.json")));
    private static final AtlasChangeGenerator ADD_TAG = atlas ->
    {
        final String atlasCountry = atlas.metaData().getCountry()
                .orElseThrow(() -> new CoreException("Atlas had no country code!"));
        validateCountry(atlasCountry);
        return Iterables.stream(atlas.areas())
                .flatMap(area -> Sets.hashSet(FeatureChange.add(CompleteArea.shallowFrom(area)
                        .withTags(area.getTags()).withAddedTag(MUTATED_KEY, MUTATED_VALUE))))
                .collectToSet();
    };
    private static final Set<FeatureChange> NEW_POINT_FEATURE_CHANGE_SET = Sets
            .hashSet(FeatureChange.add(
                    new CompletePoint(123L, SlippyTile.forName(SHARD_9_261_195).bounds().center(),
                            FOREST, Sets.hashSet())));
    private static final AtlasChangeGenerator ADD_POINT = atlas ->
    {
        final String atlasCountry = atlas.metaData().getCountry()
                .orElseThrow(() -> new CoreException("Atlas had no country code!"));
        validateCountry(atlasCountry);
        return NEW_POINT_FEATURE_CHANGE_SET;
    };
    private static final AtlasMutatorConfiguration BASIC_ATLAS_MUTATOR_CONFIGURATION = new AtlasMutatorConfiguration(
            Sets.hashSet(COUNTRY), SHARDING, BOUNDARIES,
            ResourceFileSystem.SCHEME + "://test/input", "",
            ResourceFileSystem.simpleconfiguration(), EMPTY, true, false, false);
    private static final ConfiguredAtlasChangeGenerator BASIC_MUTATOR_ADD_TAG = new BasicConfiguredAtlasChangeGenerator(
            TEST_MUTATOR_NAME, ADD_TAG, EMPTY);
    private static final ConfiguredAtlasChangeGenerator BASIC_MUTATOR_ADD_POINT = new BasicConfiguredAtlasChangeGenerator(
            TEST_MUTATOR_NAME, ADD_POINT, EMPTY);
    @Rule
    public final AtlasMutatorHelperTestRule rule = new AtlasMutatorHelperTestRule();

    private static void validateCountry(final String country)
    {
        if (!COUNTRY.equals(country))
        {
            throw new CoreException(
                    "Atlas meta data did not have correct country code: expected: {}, Actual: {}",
                    COUNTRY, country);
        }
    }

    @After
    @Before
    public void cleanup()
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
        ResourceFileSystem.clear();
    }

    @Test
    public void testAssignedToShardAppliedFeatureChanges()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlas());
        final PairFunction<Tuple2<CountryShard, List<FeatureChange>>, CountryShard, List<FeatureChange>> function = AtlasMutatorHelper
                .assignedToShardAppliedFeatureChanges(mockLevelAddTag());
        final List<FeatureChange> list1 = new ArrayList<>();
        list1.add(FeatureChange.remove(AREA_123));
        list1.add(FeatureChange.add(AREA_456));
        list1.add(FeatureChange.add(AREA_789));
        try
        {
            // No merge
            final Tuple2<CountryShard, List<FeatureChange>> merged = function
                    .call(new Tuple2<>(COUNTRY_SHARD_9_261_195, list1));
            Assert.assertEquals(3, merged._2().size());
        }
        catch (final Exception e)
        {
            throw new CoreException(MERGE_FAILED, e);
        }

        list1.add(FeatureChange.add(AREA_456));
        try
        {
            // Merge on 456
            final Tuple2<CountryShard, List<FeatureChange>> merged = function.call(
                    new Tuple2<CountryShard, List<FeatureChange>>(COUNTRY_SHARD_9_261_195, list1));
            Assert.assertEquals(3, merged._2().size());
        }
        catch (final Exception e)
        {
            throw new CoreException(MERGE_FAILED, e);
        }
    }

    @Test(expected = CoreException.class)
    public void testAssignedToShardAppliedFeatureChangesException()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlas());
        final PairFunction<Tuple2<CountryShard, List<FeatureChange>>, CountryShard, List<FeatureChange>> function = AtlasMutatorHelper
                .assignedToShardAppliedFeatureChanges(mockLevelAddTag());
        final List<FeatureChange> list1 = new ArrayList<>();
        list1.add(FeatureChange.remove(AREA_123));
        list1.add(FeatureChange.add(AREA_456));
        list1.add(FeatureChange.add(AREA_123));
        try
        {
            // Merge conflict on 123
            final Tuple2<CountryShard, List<FeatureChange>> merged = function.call(
                    new Tuple2<CountryShard, List<FeatureChange>>(COUNTRY_SHARD_9_261_195, list1));
            Assert.assertEquals(3, merged._2().size());
        }
        catch (final Exception e)
        {
            throw new CoreException(MERGE_FAILED, e);
        }
    }

    @Test
    public void testChangeAndShardToAtlasMapToAtlas()
    {
        final Atlas initial = this.rule.getChangeToAtlas();
        addAtlasToResourceFileSystem(initial);

        final ConfiguredAtlasChangeGenerator mutator = new BasicConfiguredAtlasChangeGenerator(
                TEST_MUTATOR_NAME, ADD_TAG, NO_EXPANSION_CONFIGURATION);
        final AtlasMutationLevel level = mockLevel(mutator, 1);
        level.setAllowRDD(true);
        level.setPreloadRDD(true);
        final PairFlatMapFunction<Tuple2<CountryShard, Tuple2<Change, Optional<Map<CountryShard, PackedAtlas>>>>, CountryShard, Tuple<PackedAtlas, Change>> function = AtlasMutatorHelper
                .changeAndShardToAtlasMapToAtlas(level);

        final ChangeBuilder builder = new ChangeBuilder();
        NEW_POINT_FEATURE_CHANGE_SET.forEach(builder::add);
        final Change change = builder.get();
        final CountryShard shard = COUNTRY_SHARD_9_261_195;
        final Map<CountryShard, PackedAtlas> lookupMap = new HashMap<>();
        lookupMap.put(shard, (PackedAtlas) initial);
        final Tuple2<Change, Optional<Map<CountryShard, PackedAtlas>>> changeMap = new Tuple2<>(
                change, Optional.of(lookupMap));
        final Tuple2<CountryShard, Tuple2<Change, Optional<Map<CountryShard, PackedAtlas>>>> input = new Tuple2<>(
                shard, changeMap);

        try
        {
            final List<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> result = Lists
                    .newArrayList(function.call(input));
            Assert.assertEquals(1, result.size());
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
    }

    @Test
    public void testChangeToAtlasEdge()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlasEdge());
        final AtlasMutationLevel level = mockLevelAddTag();
        final PairFlatMapFunction<Tuple2<CountryShard, Change>, CountryShard, Tuple<PackedAtlas, Change>> function = AtlasMutatorHelper
                .changeToAtlas(level);
        final Atlas atlas = this.rule.getChangeToAtlasEdge();

        // Prepare some edge FCs that will be stripped
        final FeatureChange featureChange31 = FeatureChange
                .add(new CompleteNode(5L, Location.TEST_1, Maps.hashMap(), Sets.treeSet(),
                        Sets.treeSet(11L), Sets.hashSet()));
        final FeatureChange featureChange32 = FeatureChange
                .add(new CompleteNode(6L, Location.TEST_2, Maps.hashMap(), Sets.treeSet(11L),
                        Sets.treeSet(), Sets.hashSet()));
        final FeatureChange featureChange33 = FeatureChange
                .add(new CompleteEdge(11L, new PolyLine(Location.TEST_1, Location.TEST_2),
                        Maps.hashMap(), 5L, 6L, Sets.hashSet()));

        // The edge 12... It is outside and has two FC versions. The first one is full, the second
        // one is only containing tags.
        final Rectangle bounds = SlippyTile.forName(SHARD_OUTSIDE).bounds()
                .contract(Distance.meters(100));
        final Location start = bounds.lowerLeft();
        final Location end = bounds.upperRight();
        final FeatureChange featureChange34 = FeatureChange
                .add(new CompleteEdge(12L, null, Maps.hashMap("just", "tags"), null, null, null)
                        .withBoundsExtendedBy(bounds));

        // Prepare some edge FCs that will not be stripped
        final Location withinShard = SlippyTile.forName(SHARD_9_261_195).bounds().center();
        // Get the edge outside (highway=primary) and force its polyLine inside in a FC
        final FeatureChange featureChange43 = FeatureChange.add(new CompleteEdge(250300000000L,
                PolyLine.wkt("LINESTRING (4.2162759 38.8218896, "
                        + withinShard.getLongitude().asDegrees() + " "
                        + withinShard.getLatitude().asDegrees()
                        + ", 4.2174174 38.8218741, 4.2177407 38.8219892)"),
                null, null, null, null));
        // This one is in the shard initially, but this FC moves it out completely. It should not be
        // stripped, even though the new one is gone, because it initially intersected the shard.
        // Move it to be like the primary road (slightly same polyLine and but move the nodes with
        // him)
        final Edge motorway = atlas.edge(250255000000L);
        final Edge primary = atlas.edge(250300000000L);
        final FeatureChange featureChangeMotorway = FeatureChange
                .add(CompleteEdge.shallowFrom(motorway).withPolyLine(
                        PolyLine.wkt("LINESTRING (4.2162758 38.8218896, 4.2166551 38.8220451, "
                                + "4.2174174 38.8218741, 4.2177406 38.8219892)")))
                .withAtlasContext(atlas);
        final FeatureChange oldNodeRemove1 = FeatureChange.add(CompleteNode.from(motorway.start())
                .withLocation(Location.forWkt("POINT (4.2162758 38.8218896)")));
        final FeatureChange oldNodeRemove2 = FeatureChange.add(CompleteNode.from(motorway.end())
                .withLocation(Location.forWkt("POINT (4.2177406 38.8219892)")));

        // Test within the shard
        final CountryShard shard = COUNTRY_SHARD_9_261_195;
        final Change change = ChangeBuilder.newInstance().add(featureChange31)
                // Add the shallow featureChange34 here for edge 12 to test the edge stripping
                .add(featureChange32).add(featureChange33).add(featureChange34).add(featureChange43)
                .add(featureChangeMotorway).add(oldNodeRemove1).add(oldNodeRemove2).get();
        final Tuple2<CountryShard, Change> tuple = new Tuple2<>(shard, change);
        final Tuple2<CountryShard, Tuple<PackedAtlas, Change>> result;
        try
        {
            ResourceFileSystem.printContents();
            final Iterable<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> resultIterable = Lists
                    .newArrayList(function.call(tuple));
            Assert.assertEquals(1, Iterables.size(resultIterable));
            result = resultIterable.iterator().next();
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
        final CountryShard resultShard = result._1();
        Assert.assertEquals(shard, resultShard);
        final PackedAtlas resultAtlas = result._2().getFirst();
        Assert.assertEquals(2, resultAtlas.numberOfEdges());
        // primary
        Assert.assertNotNull(resultAtlas.edge(250300000000L));
        // trunk
        Assert.assertNotNull(resultAtlas.edge(250280000000L));
        // motorway
        Assert.assertNull(resultAtlas.edge(250255000000L));

        this.addAtlasToResourceFileSystem(this.rule.getChangeToAtlasEdge(), SHARD_8_130_98);

        // Test outside the shard
        final CountryShard shard2 = COUNTRY_SHARD_8_130_98;
        final Change change2 = ChangeBuilder.newInstance().add(featureChange31).add(featureChange32)
                .add(featureChange33).add(featureChange43).get();
        final Tuple2<CountryShard, Change> tuple2 = new Tuple2<>(shard2, change2);
        final Tuple2<CountryShard, Tuple<PackedAtlas, Change>> result2;
        try
        {
            ResourceFileSystem.printContents();
            final Iterable<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> resultIterable = Lists
                    .newArrayList(function.call(tuple2));
            // The edges are outside and should have been trimmed here by the
            // "stripSecondDegreeEdges" function in AtlasMutatorHelper
            Assert.assertEquals(1, Iterables.size(resultIterable));
            result2 = resultIterable.iterator().next();
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
        final CountryShard resultShard2 = result2._1();
        Assert.assertEquals(shard2, resultShard2);
        final PackedAtlas resultAtlas2 = result2._2().getFirst();
        Assert.assertEquals(2, resultAtlas2.numberOfEdges());
        // primary
        Assert.assertNotNull(resultAtlas.edge(250300000000L));
        // trunk
        Assert.assertNotNull(resultAtlas.edge(250280000000L));
    }

    @Test
    public void testChangeToAtlasNewAtlas()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlasEdge());
        final AtlasMutationLevel level = mockLevelAddTag();
        final PairFlatMapFunction<Tuple2<CountryShard, Change>, CountryShard, Tuple<PackedAtlas, Change>> function = AtlasMutatorHelper
                .changeToAtlas(level);

        final CountryShard shard = COUNTRY_SHARD_OUTSIDE;
        final FeatureChange fullRemove = FeatureChange.remove(
                new CompletePoint(123L, shard.bounds().center(), Maps.hashMap(), Sets.hashSet()));
        final FeatureChange fullAdd = FeatureChange.add(
                new CompletePoint(456L, shard.bounds().center(), Maps.hashMap(), Sets.hashSet()));

        // Full Remove only
        final Change change = ChangeBuilder.newInstance().add(fullRemove).get();
        final Tuple2<CountryShard, Change> tuple = new Tuple2<>(shard, change);
        try
        {
            ResourceFileSystem.printContents();
            final Iterable<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> resultIterable = Lists
                    .newArrayList(function.call(tuple));
            Assert.assertEquals(0, Iterables.size(resultIterable));
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }

        // Full Remove and Full Add
        final Change change2 = ChangeBuilder.newInstance().add(fullAdd).get();
        final Tuple2<CountryShard, Change> tuple2 = new Tuple2<>(shard, change2);
        final Tuple2<CountryShard, Tuple<PackedAtlas, Change>> result;
        try
        {
            ResourceFileSystem.printContents();
            final Iterable<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> resultIterable = Lists
                    .newArrayList(function.call(tuple2));
            Assert.assertEquals(1, Iterables.size(resultIterable));
            result = resultIterable.iterator().next();
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }

        final Atlas resultAtlas = result._2().getFirst();
        Assert.assertEquals(1, resultAtlas.numberOfPoints());
        Assert.assertNotNull(resultAtlas.point(456L));
    }

    @Test
    public void testChangeToAtlasRelation()
    {
        final Atlas source = new AtlasResourceLoader()
                .load(new InputStreamResource(() -> AtlasMutatorHelperTest.class
                        .getResourceAsStream("changeToAtlasRelation.atlas.txt")));
        addAtlasToResourceFileSystem(source);
        final AtlasMutationLevel level = mockLevelAddTag();
        final PairFlatMapFunction<Tuple2<CountryShard, Change>, CountryShard, Tuple<PackedAtlas, Change>> function = AtlasMutatorHelper
                .changeToAtlas(level);

        addAtlasToResourceFileSystem(source, SHARD_9_261_195);

        // Update group and remove forest from members
        final FeatureChange updateGroup = FeatureChange.add(CompleteRelation
                .from(source.relation(250375000000L)).withRemovedMember(source.area(250354000000L))
                .withAddedTag(AtlasMutator.MUTATOR_META_DATA_KEY
                        + AtlasMutator.MUTATOR_META_DATA_SPLIT + "UnitTest", "1"));
        final FeatureChange updateForest = FeatureChange.add(CompleteArea
                .from(source.area(250354000000L)).withRemovedRelationIdentifier(250375000000L));

        // Update water and remove pond
        final FeatureChange updateWater = FeatureChange.add(CompleteRelation
                .from(source.relation(250410000000L)).withRemovedMember(source.area(250383000000L))
                .withAddedTag(AtlasMutator.MUTATOR_META_DATA_KEY
                        + AtlasMutator.MUTATOR_META_DATA_SPLIT + "UnitTest", "1"));
        final FeatureChange updatePond = FeatureChange.add(CompleteArea
                .from(source.area(250383000000L)).withRemovedRelationIdentifier(250410000000L));

        /*
         * Create a brand new relation and add the mutator: tag to it. We'll source it from
         * 250375000000 to get some data but then overwrite it with a new ID.
         */
        final CompleteRelation newRelation = CompleteRelation.from(source.relation(250375000000L))
                .withAddedTag(AtlasMutator.MUTATOR_META_DATA_KEY
                        + AtlasMutator.MUTATOR_META_DATA_SPLIT + "UnitTest", "1");
        newRelation.withIdentifier(new EntityIdentifierGenerator().generateIdentifier(newRelation));
        final FeatureChange createGroupRelation = FeatureChange.add(newRelation);

        /*
         * Create a brand new relation that is identical to a created relation from a previous
         * level. We want to then check and make sure that *both* mutator: tags on this relation are
         * saved.
         */
        final CompleteRelation identicalCreatedRelation = CompleteRelation
                .from(source.relation(7585456629725215238L));
        identicalCreatedRelation.withIdentifier(
                new EntityIdentifierGenerator().generateIdentifier(identicalCreatedRelation));
        identicalCreatedRelation.withAddedTag(AtlasMutator.MUTATOR_META_DATA_KEY
                + AtlasMutator.MUTATOR_META_DATA_SPLIT + "UnitTest", "1");
        final FeatureChange createIdenticalRelation = FeatureChange.add(identicalCreatedRelation);

        // Test within the shard
        final CountryShard shard = COUNTRY_SHARD_9_261_195;
        final Change change = ChangeBuilder.newInstance().add(updateGroup).add(updateForest)
                .add(updateWater).add(updatePond).add(createGroupRelation)
                .add(createIdenticalRelation).get();
        final Tuple2<CountryShard, Change> tuple = new Tuple2<>(shard, change);
        final Tuple2<CountryShard, Tuple<PackedAtlas, Change>> result;
        try
        {
            ResourceFileSystem.printContents();
            final Iterable<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> resultIterable = Lists
                    .newArrayList(function.call(tuple));
            Assert.assertEquals(1, Iterables.size(resultIterable));
            result = resultIterable.iterator().next();
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
        final CountryShard resultShard = result._1();
        Assert.assertEquals(shard, resultShard);
        final PackedAtlas resultAtlas = result._2().getFirst();

        Assert.assertEquals(2, resultAtlas.numberOfAreas());
        // forest
        Assert.assertNotNull(resultAtlas.area(250354000000L));
        // building
        Assert.assertNotNull(resultAtlas.area(250338000000L));
        // lake
        Assert.assertNull(resultAtlas.edge(250312000000L));

        Assert.assertEquals(3, resultAtlas.numberOfRelations());
        Assert.assertNotNull(resultAtlas.relation(250375000000L));
        Assert.assertEquals(1, resultAtlas.relation(250375000000L).members().size());
        Assert.assertEquals(250338000000L,
                resultAtlas.relation(250375000000L).members().get(0).getEntity().getIdentifier());
        resultAtlas.relation(250375000000L).getTags()
                .forEach((key, value) -> Assert.assertFalse(
                        "Relation was found to have mutator tag, even though it should not",
                        key.startsWith(AtlasMutator.MUTATOR_META_DATA_KEY
                                + AtlasMutator.MUTATOR_META_DATA_SPLIT)));
        Assert.assertTrue(
                "Relation was not found to have a tag with `mutator:' prefix, even though it should",
                resultAtlas.relation(newRelation.getIdentifier()).getTags().entrySet().stream()
                        .anyMatch(entry -> entry.getKey()
                                .startsWith(AtlasMutator.MUTATOR_META_DATA_KEY
                                        + AtlasMutator.MUTATOR_META_DATA_SPLIT)));

        // Identical created relation should have both mutator tags
        Assert.assertTrue(resultAtlas.relation(identicalCreatedRelation.getIdentifier())
                .getTag(AtlasMutator.MUTATOR_META_DATA_KEY + AtlasMutator.MUTATOR_META_DATA_SPLIT
                        + "FromJosm")
                .isPresent());
        Assert.assertTrue(resultAtlas.relation(identicalCreatedRelation.getIdentifier())
                .getTag(AtlasMutator.MUTATOR_META_DATA_KEY + AtlasMutator.MUTATOR_META_DATA_SPLIT
                        + "UnitTest")
                .isPresent());
    }

    @Test
    public void testChangeToAtlasSimple()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlas());
        final AtlasMutationLevel level = mockLevelAddTag();
        final PairFlatMapFunction<Tuple2<CountryShard, Change>, CountryShard, Tuple<PackedAtlas, Change>> function = AtlasMutatorHelper
                .changeToAtlas(level);

        final Atlas source = this.rule.getChangeToAtlas();
        addAtlasToResourceFileSystem(source, SHARD_9_261_195);

        // Test within the shard
        final CountryShard shard = COUNTRY_SHARD_9_261_195;
        final Area area = source.areas().iterator().next();
        final FeatureChange featureChange = FeatureChange.add(CompleteArea.shallowFrom(area)
                .withTags(area.getTags()).withAddedTag(MUTATED_KEY, MUTATED_VALUE));
        final Change change = ChangeBuilder.newInstance().add(featureChange).get();
        final Tuple2<CountryShard, Change> tuple = new Tuple2<>(shard, change);
        final Tuple2<CountryShard, Tuple<PackedAtlas, Change>> result;
        try
        {
            ResourceFileSystem.printContents();
            final Iterable<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> resultIterable = Lists
                    .newArrayList(function.call(tuple));
            Assert.assertEquals(1, Iterables.size(resultIterable));
            result = resultIterable.iterator().next();
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
        final CountryShard resultShard = result._1();
        Assert.assertEquals(shard, resultShard);
        final PackedAtlas resultAtlas = result._2().getFirst();
        Assert.assertFalse(
                resultAtlas.areas().iterator().next().getTags(MUTATED_KEY::equals).isEmpty());

        // Test outside the shard
        final CountryShard shard2 = COUNTRY_SHARD_8_130_98;
        final FeatureChange featureChange2 = FeatureChange
                .add(CompleteArea.from(area).withAddedTag(MUTATED_KEY, MUTATED_VALUE));
        final Change change2 = ChangeBuilder.newInstance().add(featureChange2).get();
        final Tuple2<CountryShard, Change> tuple2 = new Tuple2<>(shard2, change2);
        final Tuple2<CountryShard, Tuple<PackedAtlas, Change>> result2;
        try
        {
            ResourceFileSystem.printContents();
            final Iterable<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> resultIterable = Lists
                    .newArrayList(function.call(tuple2));
            // The edges are outside and should have been trimmed here by the
            // "stripSecondDegreeEdges" function in AtlasMutatorHelper
            Assert.assertEquals(1, Iterables.size(resultIterable));
            result2 = resultIterable.iterator().next();
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
        final CountryShard resultShard2 = result2._1();
        Assert.assertEquals(shard2, resultShard2);
        final PackedAtlas resultAtlas2 = result2._2().getFirst();
        Assert.assertEquals(1, resultAtlas2.numberOfAreas());
        Assert.assertFalse(
                resultAtlas2.areas().iterator().next().getTags(MUTATED_KEY::equals).isEmpty());
    }

    @Test
    public void testFeatureChangeListToChange()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlas());
        final AtlasMutationLevel level = mockLevelAddTag();
        final PairFunction<Tuple2<CountryShard, List<FeatureChange>>, CountryShard, Change> function = AtlasMutatorHelper
                .featureChangeListToChange(level);

        final CountryShard shard = COUNTRY_SHARD_9_261_195;
        final FeatureChange featureChange1 = FeatureChange
                .add(new CompleteArea(123L, Polygon.SILICON_VALLEY, Maps.hashMap(), Sets.hashSet())
                        .withAddedTag(MUTATED_KEY, MUTATED_VALUE));
        final FeatureChange featureChange2 = featureChange1;
        final FeatureChange featureChange3 = FeatureChange
                .add(new CompleteLine(123L, PolyLine.TEST_POLYLINE, Maps.hashMap(), Sets.hashSet())
                        .withAddedTag(MUTATED_KEY, MUTATED_VALUE));
        final List<FeatureChange> featureChanges = new ArrayList<>();
        featureChanges.add(featureChange1);
        featureChanges.add(featureChange2);
        featureChanges.add(featureChange3);
        final Tuple2<CountryShard, List<FeatureChange>> tupleInput = new Tuple2<>(shard,
                featureChanges);
        final Tuple2<CountryShard, Change> tupleOutput;
        try
        {
            tupleOutput = function.call(tupleInput);
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
        final CountryShard outputShard = tupleOutput._1();
        final Change outputChange = tupleOutput._2();
        Assert.assertEquals(shard, outputShard);
        // FeatureChange1 and 2 should be merged in one.
        Assert.assertEquals(2, outputChange.changeCount());
    }

    @Test
    public void testShardFeatureChangesToAssignedShardFeatureChanges()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlas());
        final AtlasMutationLevel level = mockLevelAddTag();
        final PairFlatMapFunction<Tuple2<CountryShard, List<FeatureChange>>, CountryShard, List<FeatureChange>> function = AtlasMutatorHelper
                .shardFeatureChangesToAssignedShardFeatureChanges(level);

        final Atlas initial = this.rule.getChangeToAtlas();
        final Area area = initial.areas().iterator().next();
        final FeatureChange featureChange = FeatureChange.add(CompleteArea.shallowFrom(area)
                .withTags(area.getTags()).withAddedTag(MUTATED_KEY, MUTATED_VALUE));
        final CountryShard shard1 = COUNTRY_SHARD_9_261_195;
        final CountryShard shard2 = COUNTRY_SHARD_8_130_98;
        final Tuple2<CountryShard, List<FeatureChange>> tupleInput = new Tuple2<>(shard1,
                Lists.newArrayList(featureChange));
        final Iterable<Tuple2<CountryShard, List<FeatureChange>>> resultTuples;
        try
        {
            resultTuples = Lists.newArrayList(function.call(tupleInput));
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
        Assert.assertEquals(2, Iterables.size(resultTuples));
        final Set<CountryShard> resultShards = Iterables.stream(resultTuples).map(Tuple2::_1)
                .collectToSet();
        Assert.assertTrue(resultShards.contains(shard1));
        Assert.assertTrue(resultShards.contains(shard2));
        for (final Tuple2<CountryShard, List<FeatureChange>> resultTuple : resultTuples)
        {
            final List<FeatureChange> resultList = resultTuple._2();
            Assert.assertEquals(1, resultList.size());
            Assert.assertEquals(featureChange, resultList.get(0));
        }
    }

    @Test
    public void testShardToAtlasMapToFeatureChanges()
    {
        final Atlas initial = this.rule.getChangeToAtlas();
        addAtlasToResourceFileSystem(initial);
        final ConfiguredAtlasChangeGenerator mutator = new BasicConfiguredAtlasChangeGenerator(
                TEST_MUTATOR_NAME, ADD_TAG, NO_EXPANSION_CONFIGURATION);
        final AtlasMutationLevel level = mockLevel(mutator, 1);
        level.setAllowRDD(true);
        level.setPreloadRDD(true);
        final PairFlatMapFunction<Tuple2<CountryShard, Map<CountryShard, PackedAtlas>>, CountryShard, List<FeatureChange>> function = AtlasMutatorHelper
                .shardToAtlasMapToFeatureChanges(level);

        final CountryShard shard1 = COUNTRY_SHARD_9_261_195;
        final Map<CountryShard, PackedAtlas> lookupMap1 = new HashMap<>();
        lookupMap1.put(shard1, (PackedAtlas) initial);
        final Tuple2<CountryShard, Map<CountryShard, PackedAtlas>> input1 = new Tuple2<>(shard1,
                lookupMap1);

        try
        {
            final Iterable<Tuple2<CountryShard, List<FeatureChange>>> result1 = Lists
                    .newArrayList(function.call(input1));
            Assert.assertEquals(1, Iterables.size(result1));
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
    }

    @Test
    public void testShardToFeatureChanges()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlas());
        final AtlasMutationLevel level = mockLevelAddTag();
        final PairFlatMapFunction<CountryShard, CountryShard, List<FeatureChange>> function = AtlasMutatorHelper
                .shardToFeatureChanges(level);

        final CountryShard shard1 = COUNTRY_SHARD_9_261_195;
        final CountryShard shard2 = COUNTRY_SHARD_8_130_98;
        final CountryShard shard3 = COUNTRY_SHARD_8_131_97;

        try
        {
            final Iterable<Tuple2<CountryShard, List<FeatureChange>>> result1 = Lists
                    .newArrayList(function.call(shard1));
            Assert.assertEquals(1, Iterables.size(result1));
            final Iterable<Tuple2<CountryShard, List<FeatureChange>>> result2 = Lists
                    .newArrayList(function.call(shard2));
            Assert.assertEquals(0, Iterables.size(result2));
            final Iterable<Tuple2<CountryShard, List<FeatureChange>>> result3 = Lists
                    .newArrayList(function.call(shard3));
            Assert.assertEquals(0, Iterables.size(result3));
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
    }

    @Test
    public void testShardToFeatureChangesNewFeature()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlas());
        final AtlasMutationLevel level = mockLevelAddPoint();
        final PairFlatMapFunction<CountryShard, CountryShard, List<FeatureChange>> function = AtlasMutatorHelper
                .shardToFeatureChanges(level);

        final CountryShard shard = COUNTRY_SHARD_9_261_195;
        try
        {
            final Iterable<Tuple2<CountryShard, List<FeatureChange>>> result = Lists
                    .newArrayList(function.call(shard));
            Assert.assertEquals(1, Iterables.size(result));
            Assert.assertEquals("XYZ",
                    result.iterator().next()._2().get(0).getAfterView().tag(ISOCountryTag.KEY));
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }

    }

    @Test
    public void testUntouchedShardAndMapToPotentialSourcePackedAtlas()
    {
        final Atlas initial = this.rule.getChangeToAtlas();
        addAtlasToResourceFileSystem(initial);

        final ConfiguredAtlasChangeGenerator mutator = new BasicConfiguredAtlasChangeGenerator(
                TEST_MUTATOR_NAME, ADD_TAG, NO_EXPANSION_CONFIGURATION);
        final AtlasMutationLevel level = mockLevel(mutator, 1);
        level.setAllowRDD(true);
        level.setPreloadRDD(true);
        final PairFlatMapFunction<Tuple2<CountryShard, Map<CountryShard, PackedAtlas>>, CountryShard, PackedAtlas> function = AtlasMutatorHelper
                .untouchedShardAndMapToPotentialSourcePackedAtlas(level);

        final CountryShard shard = COUNTRY_SHARD_9_261_195;
        final Map<CountryShard, PackedAtlas> lookupMap = new HashMap<>();
        lookupMap.put(shard, (PackedAtlas) initial);
        final Tuple2<CountryShard, Map<CountryShard, PackedAtlas>> input = new Tuple2<>(shard,
                lookupMap);

        try
        {
            final List<Tuple2<CountryShard, PackedAtlas>> result = Lists
                    .newArrayList(function.call(input));
            Assert.assertEquals(1, result.size());
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
    }

    @Test
    public void testUntouchedShardToPotentialSourcePackedAtlas()
    {
        addAtlasToResourceFileSystem(this.rule.getChangeToAtlas());
        final AtlasMutationLevel level = mockLevelAddTag();
        final PairFlatMapFunction<CountryShard, CountryShard, PackedAtlas> function = AtlasMutatorHelper
                .untouchedShardToPotentialSourcePackedAtlas(level);
        final CountryShard shard = COUNTRY_SHARD_9_261_195;
        try
        {
            final Iterable<Tuple2<CountryShard, PackedAtlas>> result = Lists
                    .newArrayList(function.call(shard));
            Assert.assertEquals(1, Iterables.size(result));
            final Tuple2<CountryShard, PackedAtlas> tuple = result.iterator().next();
            Assert.assertEquals(shard, tuple._1());
            Assert.assertEquals(this.rule.getChangeToAtlas(), tuple._2());
        }
        catch (final Exception e)
        {
            throw new CoreException(TEST_FAILED, e);
        }
    }

    private void addAtlasToResourceFileSystem(final Atlas atlas)
    {
        addAtlasToResourceFileSystem(atlas, SHARD_9_261_195);
    }

    private void addAtlasToResourceFileSystem(final Atlas atlas, final String shardName)
    {
        final ByteArrayResource atlasResource = new ByteArrayResource();
        atlas.save(atlasResource);
        ResourceFileSystem.addResource(ResourceFileSystem.SCHEME + "://test/input/" + COUNTRY + "/"
                + COUNTRY + "_" + shardName + ".atlas", atlasResource);
    }

    private AtlasMutationLevel mockLevel(final ConfiguredAtlasChangeGenerator mutator,
            final int levelIndex)
    {
        return new AtlasMutationLevel(BASIC_ATLAS_MUTATOR_CONFIGURATION, GROUP,
                Sets.hashSet(COUNTRY), Sets.hashSet(mutator), levelIndex, levelIndex);
    }

    private AtlasMutationLevel mockLevelAddPoint()
    {
        return mockLevel(BASIC_MUTATOR_ADD_POINT, 0);
    }

    private AtlasMutationLevel mockLevelAddTag()
    {
        return mockLevel(BASIC_MUTATOR_ADD_TAG, 0);
    }
}
