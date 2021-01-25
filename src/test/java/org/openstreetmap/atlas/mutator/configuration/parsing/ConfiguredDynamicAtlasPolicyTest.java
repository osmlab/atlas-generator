package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.util.function.Predicate;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEdge;
import org.openstreetmap.atlas.geography.atlas.dynamic.DynamicAtlas;
import org.openstreetmap.atlas.geography.atlas.dynamic.policy.DynamicAtlasPolicy;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.mutator.AtlasMutatorHelperTest;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfiguredFilter;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;

/**
 * @author matthieun
 */
public class ConfiguredDynamicAtlasPolicyTest
{
    public static final String INPUT = "resource://test/input";
    public static final String COUNTRY = "XYZ";
    public static final String Z_8_X_130_Y_98 = "/XYZ/XYZ_8-130-98.atlas";
    public static final String Z_8_X_131_Y_97 = "/XYZ/XYZ_8-131-97.atlas";
    public static final String Z_8_X_131_Y_97_INPUT_DEPENDENCY = "_inputDependency_highwayPrimaryInputDependency/XYZ/XYZ_8-131-97.atlas";
    public static final String Z_8_X_131_Y_98 = "/XYZ/XYZ_8-131-98.atlas";
    public static final String Z_9_X_261_Y_195 = "/XYZ/XYZ_9-261-195.atlas";
    public static final Shard SHARD_9_261_195 = SlippyTile.forName("9-261-195");

    @Rule
    public final ConfiguredDynamicAtlasPolicyTestRule rule = new ConfiguredDynamicAtlasPolicyTestRule();

    @After
    public void destroy()
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
    }

    @Before
    public void prepare()
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
    }

    @Test
    public void testExpansion()
    {
        final long size = 8192;
        final ByteArrayResource resource1 = new ByteArrayResource(size);
        final Atlas shard1 = this.rule.getZ9X261Y195();
        shard1.save(resource1);
        ResourceFileSystem.addResource(INPUT + Z_9_X_261_Y_195, resource1);
        final ByteArrayResource resource2 = new ByteArrayResource(size);
        final Atlas shard2 = this.rule.getZ8X131Y97();
        shard2.save(resource2);
        ResourceFileSystem.addResource(INPUT + Z_8_X_131_Y_97, resource2);
        final ByteArrayResource resource3 = new ByteArrayResource(size);
        final Atlas shard3 = this.rule.getZ8X131Y98();
        shard3.save(resource3);
        ResourceFileSystem.addResource(INPUT + Z_8_X_131_Y_98, resource3);

        final Edge edgeJunctionRoundabout = new CompleteEdge(123L, null,
                Maps.hashMap("junction", "roundabout"), null, null, null);
        final Edge edgeRouteFerry = new CompleteEdge(123L, null, Maps.hashMap("route", "ferry"),
                null, null, null);
        final Edge edgeHighwayPrimary = new CompleteEdge(123L, null,
                Maps.hashMap("highway", "primary"), null, null, null);

        final Configuration configuration = new StandardConfiguration(new InputStreamResource(
                () -> ConfiguredDynamicAtlasPolicyTest.class.getResourceAsStream(
                        ConfiguredDynamicAtlasPolicyTest.class.getSimpleName() + ".json")));

        final ConfiguredDynamicAtlasPolicy junctionRoundaboutConfiguredDynamicAtlasPolicy = ConfiguredDynamicAtlasPolicy
                .from("junctionRoundaboutDynamicAtlasPolicy", configuration);
        final Sharding sharding = AtlasMutatorHelperTest.SHARDING;
        final DynamicAtlasPolicy junctionRoundaboutDynamicAtlasPolicy = junctionRoundaboutConfiguredDynamicAtlasPolicy
                .getPolicy(Sets.hashSet(SHARD_9_261_195), sharding, INPUT,
                        ResourceFileSystem.simpleconfiguration(), COUNTRY);
        Assert.assertEquals(Sets.hashSet(SHARD_9_261_195),
                junctionRoundaboutDynamicAtlasPolicy.getInitialShards());
        Assert.assertFalse(junctionRoundaboutDynamicAtlasPolicy.isExtendIndefinitely());
        Assert.assertTrue(junctionRoundaboutDynamicAtlasPolicy.isDeferLoading());
        Assert.assertFalse(junctionRoundaboutDynamicAtlasPolicy.isAggressivelyExploreRelations());
        final Predicate<AtlasEntity> junctionRoundaboutEntitiesToConsiderForExpansion = junctionRoundaboutDynamicAtlasPolicy
                .getAtlasEntitiesToConsiderForExpansion();
        Assert.assertTrue(
                junctionRoundaboutEntitiesToConsiderForExpansion.test(edgeJunctionRoundabout));
        Assert.assertFalse(junctionRoundaboutEntitiesToConsiderForExpansion.test(edgeRouteFerry));

        // Add the pre-filtered neighbor shard for the DynamicAtlas to fetch. Contains a
        // highway=primary roundabout and a highway=primary
        final ByteArrayResource resource2InputDependency = new ByteArrayResource(size);
        shard2.subAtlas(ConfiguredFilter.from("highwayPrimaryFilter", configuration),
                AtlasCutType.SOFT_CUT).orElseThrow(() -> new CoreException("Impossible"))
                .save(resource2InputDependency);
        ResourceFileSystem.addResource(INPUT + Z_8_X_131_Y_97_INPUT_DEPENDENCY,
                resource2InputDependency);

        final DynamicAtlas dynamicAtlasJunctionRoundabout = new DynamicAtlas(
                junctionRoundaboutDynamicAtlasPolicy);
        dynamicAtlasJunctionRoundabout.preemptiveLoad();
        // The highway=footway in 8-131-98 should not be there as this shard should not be brought
        // in.
        Assert.assertEquals(6, dynamicAtlasJunctionRoundabout.numberOfEdges());

        final ConfiguredDynamicAtlasPolicy highwayPrimaryConfiguredDynamicAtlasPolicy = ConfiguredDynamicAtlasPolicy
                .from("highwayPrimaryDynamicAtlasPolicy", configuration);
        final DynamicAtlasPolicy highwayPrimaryDynamicAtlasPolicy = highwayPrimaryConfiguredDynamicAtlasPolicy
                .getPolicy(Sets.hashSet(SHARD_9_261_195), sharding, INPUT,
                        ResourceFileSystem.simpleconfiguration(), COUNTRY);
        Assert.assertEquals(Sets.hashSet(SHARD_9_261_195),
                highwayPrimaryDynamicAtlasPolicy.getInitialShards());
        Assert.assertFalse(highwayPrimaryDynamicAtlasPolicy.isExtendIndefinitely());
        Assert.assertTrue(highwayPrimaryDynamicAtlasPolicy.isDeferLoading());
        Assert.assertFalse(highwayPrimaryDynamicAtlasPolicy.isAggressivelyExploreRelations());
        final Predicate<AtlasEntity> highwayPrimaryEntitiesToConsiderForExpansion = highwayPrimaryDynamicAtlasPolicy
                .getAtlasEntitiesToConsiderForExpansion();
        Assert.assertFalse(
                highwayPrimaryEntitiesToConsiderForExpansion.test(edgeJunctionRoundabout));
        Assert.assertFalse(highwayPrimaryEntitiesToConsiderForExpansion.test(edgeRouteFerry));
        Assert.assertTrue(highwayPrimaryEntitiesToConsiderForExpansion.test(edgeHighwayPrimary));

        final DynamicAtlas dynamicAtlasHighwayPrimary = new DynamicAtlas(
                highwayPrimaryDynamicAtlasPolicy);
        dynamicAtlasHighwayPrimary.preemptiveLoad();
        // The highway=footway in 8-131-98 should not be there as this shard should not be brought
        // in.
        Assert.assertEquals(6, dynamicAtlasHighwayPrimary.numberOfEdges());
    }
}
