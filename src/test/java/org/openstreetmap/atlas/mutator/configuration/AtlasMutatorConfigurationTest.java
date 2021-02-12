package org.openstreetmap.atlas.mutator.configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.Validate;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.geography.atlas.change.testing.AtlasChangeGeneratorAddTurnRestrictions;
import org.openstreetmap.atlas.geography.atlas.change.testing.AtlasChangeGeneratorRemoveReverseEdges;
import org.openstreetmap.atlas.geography.atlas.change.testing.AtlasChangeGeneratorSplitRoundabout;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteArea;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEdge;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.sharding.GeoHashSharding;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.SecureAqlChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.AqlConfigInfo;
import org.openstreetmap.atlas.mutator.configuration.util.consts.AqlConstants;
import org.openstreetmap.atlas.mutator.testing.ConfiguredBroadcastableAtlasSharding;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author matthieun
 * @author Yazad Khambata
 */
public class AtlasMutatorConfigurationTest
{
    private static final Logger log = LoggerFactory.getLogger(AtlasMutatorConfigurationTest.class);

    private static final String ABC = "ABC";
    private static final String XYZ = "XYZ";
    private static final String DEF = "DEF";
    private static final String GHI = "GHI";
    private static final String GROUP0 = AtlasMutatorConfiguration.GROUP_PREFIX + "0";

    @Test
    public void testBroadcastVariable()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevelsExtended(
                "testBroadcastVariable.json");
        // All end up in the same group
        Assert.assertEquals(1, countryToMutationLevels.size());
        final ConfiguredAtlasChangeGenerator sample = countryToMutationLevels.get(GROUP0).iterator()
                .next().getMutators().iterator().next();

        Assert.assertEquals(2, sample.getBroadcastVariablesNeeded().keySet().size());
        Assert.assertTrue(sample.getBroadcastVariablesNeeded().containsKey("geohash4"));
        Assert.assertTrue(sample.getBroadcastVariablesNeeded().containsKey("geohash6"));
        final ConfiguredBroadcastableAtlasSharding geohash4 = (ConfiguredBroadcastableAtlasSharding) sample
                .getBroadcastVariablesNeeded().get("geohash4");
        Assert.assertEquals(4, ((GeoHashSharding) geohash4.read(Maps.hashMap())).getPrecision());
    }

    @Test
    public void testExcludedCountries()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevelsExtended(
                "testExcludedCountries.json");
        Assert.assertEquals(1, countryToMutationLevels.size());
        final List<AtlasMutationLevel> group0Levels = countryToMutationLevels.get(GROUP0);
        Assert.assertNotNull(group0Levels);
        for (final AtlasMutationLevel level : group0Levels)
        {
            Assert.assertEquals(Sets.hashSet(ABC, XYZ), level.getCountries());
        }
    }

    @Test(expected = CoreException.class)
    public void testExcludedCountriesError()
    {
        countryToMutationLevelsExtended("testExcludedCountriesError.json");
    }

    @Test
    public void testLoadConfiguration()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevels(
                "testLoadConfiguration.json");
        Assert.assertEquals(2, countryToMutationLevels.size());
        final List<AtlasMutationLevel> abcLevels = countryToMutationLevels.get(ABC);
        Assert.assertEquals(1, abcLevels.size());
        final AtlasMutationLevel abcLevel = abcLevels.get(0);
        Assert.assertEquals(1, abcLevel.getMutators().size());
        final AtlasChangeGenerator mutator = abcLevel.getMutators().iterator().next();
        Assert.assertEquals(AtlasChangeGeneratorAddTurnRestrictions.class.getSimpleName(),
                mutator.getName());

        final List<AtlasMutationLevel> xyzLevels = countryToMutationLevels.get(XYZ);
        Assert.assertEquals(2, xyzLevels.size());
        final AtlasMutationLevel xyzLevel1 = xyzLevels.get(0);
        final AtlasMutationLevel xyzLevel2 = xyzLevels.get(1);
        Assert.assertEquals(1, xyzLevel1.getMutators().size());
        Assert.assertEquals(2, xyzLevel2.getMutators().size());
        final AtlasChangeGenerator mutator1 = xyzLevel1.getMutators().iterator().next();
        Assert.assertEquals(AtlasChangeGeneratorSplitRoundabout.class.getSimpleName(),
                mutator1.getName());
        final Set<String> mutatorNames2 = xyzLevel2.getMutators().stream()
                .map(AtlasChangeGenerator::getName).collect(Collectors.toSet());
        final Set<String> expectedMutatorNames2 = Sets.hashSet(
                AtlasChangeGeneratorAddTurnRestrictions.class.getSimpleName(),
                AtlasChangeGeneratorRemoveReverseEdges.class.getSimpleName());
        Assert.assertEquals(expectedMutatorNames2, mutatorNames2);
    }

    @Test
    public void testLoadConfigurationDisabled()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevels(
                "testDisabled.json");
        Assert.assertEquals(1, countryToMutationLevels.size());
        final List<AtlasMutationLevel> xyzLevels = countryToMutationLevels.get(XYZ);
        Assert.assertEquals(1, xyzLevels.size());
    }

    @Test
    public void testLoadConfigurationNotConfigurable()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevels(
                "testNotConfigurable.json");
        Assert.assertEquals(1, countryToMutationLevels.size());
        final List<AtlasMutationLevel> group0Levels = countryToMutationLevels.get(GROUP0);
        Assert.assertNotNull(group0Levels);
        for (final AtlasMutationLevel level : group0Levels)
        {
            Assert.assertEquals(Sets.hashSet(ABC, XYZ), level.getCountries());
        }
    }

    @Test
    public void testLoadConfigurationNotMutatorDisabled()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevels(
                "testNotMutatorDisabled.json");
        Assert.assertEquals(0, countryToMutationLevels.size());
    }

    @Test(expected = CoreException.class)
    public void testLoadConfigurationNotMutatorEnabled()
    {
        countryToMutationLevels("testNotMutatorEnabled.json");
    }

    @Test
    public void testLoadConfigurationWithAql()
    {
        final String secret = "dummy_secret";
        System.setProperty(AqlConstants.SYSTEM_KEY, secret);

        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevels(
                "testLoadConfigurationWithAql.json");

        final List<AtlasMutationLevel> atlasMutationLevels = countryToMutationLevels.get(XYZ);
        log.info("atlasMutationLevels: {}", atlasMutationLevels);
        Validate.notEmpty(atlasMutationLevels);
        Assert.assertEquals(5, atlasMutationLevels.size());

        IntStream.range(2, 5).forEach(index ->
        {
            final AtlasMutationLevel atlasMutationLevelN = atlasMutationLevels.get(index);
            final Set<ConfiguredAtlasChangeGenerator> mutators = atlasMutationLevelN.getMutators();
            log.info("mutators: {}.", mutators);
            Assert.assertEquals(1, mutators.size());
            final ConfiguredAtlasChangeGenerator configuredAtlasChangeGenerator = mutators
                    .iterator().next();
            Assert.assertEquals(SecureAqlChangeGenerator.class,
                    configuredAtlasChangeGenerator.getClass());

            final SecureAqlChangeGenerator aqlChangeGenerator = (SecureAqlChangeGenerator) configuredAtlasChangeGenerator;
            final List<AqlConfigInfo> aqlConfigInfos = aqlChangeGenerator.getAqlConfigInfos();

            Assert.assertTrue(!CollectionUtils.isEmpty(aqlConfigInfos));
            Assert.assertTrue(aqlConfigInfos.size() >= 1);
        });
    }

    @Test
    public void testLoadInputDependency()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevels(
                "testLoadInputDependency.json");
        final List<AtlasMutationLevel> xyzLevels = countryToMutationLevels.get(XYZ);
        Assert.assertEquals(3, xyzLevels.size());
        final AtlasMutationLevel xyzLevel1 = xyzLevels.get(0);
        final AtlasMutationLevel xyzLevel2 = xyzLevels.get(1);
        final AtlasMutationLevel xyzLevel3 = xyzLevels.get(2);
        final Set<InputDependency> inputDependenciesToProvide1 = xyzLevel1
                .getInputDependenciesToProvide();
        final Set<InputDependency> inputDependenciesToRequest2 = xyzLevel2
                .getInputDependenciesToRequest();
        final Set<InputDependency> inputDependenciesToProvide2 = xyzLevel2
                .getInputDependenciesToProvide();
        Assert.assertEquals(inputDependenciesToRequest2, inputDependenciesToProvide1);
        final Set<String> dependencyPaths = inputDependenciesToProvide1.stream()
                .map(InputDependency::getPathName).collect(Collectors.toSet());
        final Set<String> expectedDependencyPaths = Sets.hashSet("edgeNodeOnly");
        Assert.assertEquals(expectedDependencyPaths, dependencyPaths);
        for (final InputDependency inputDependency : inputDependenciesToProvide2)
        {
            if ("roundaboutsOnly".equals(inputDependency.getPathName()))
            {
                final Predicate<AtlasEntity> predicate = inputDependency.getSubAtlas()
                        .getSubAtlasFilter();
                Assert.assertTrue(predicate.test(new CompleteEdge(123L, null,
                        Maps.hashMap("junction", "roundabout"), null, null, null)));
                Assert.assertFalse(predicate.test(new CompleteEdge(123L, null,
                        Maps.hashMap("junction", "somethingelse"), null, null, null)));
                Assert.assertFalse(predicate.test(new CompleteArea(123L, null, null, null)));
            }
        }
    }

    @Test
    public void testOverrideWithGroups()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevelsExtended(
                "testOverride.json");
        // Here ABC has a configuration override, meaning it should be with a separate group
        Assert.assertEquals(2, countryToMutationLevels.size());
    }

    @Test
    public void testSameInputDependency()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevels(
                "testSameInputDependency.json");
        final List<AtlasMutationLevel> xyzLevels = countryToMutationLevels.get(XYZ);
        Assert.assertEquals(2, xyzLevels.size());
    }

    @Test
    public void testSplitOrderingConfiguration()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = countryToMutationLevels(
                "testLevelSplitOrderingConfiguration.json");
        Assert.assertEquals(2, countryToMutationLevels.size());

        final List<AtlasMutationLevel> xyzLevels = countryToMutationLevels.get(XYZ);
        Assert.assertEquals(3, xyzLevels.size());
        final AtlasMutationLevel xyzLevel1 = xyzLevels.get(0);
        final AtlasMutationLevel xyzLevel2 = xyzLevels.get(1);
        final AtlasMutationLevel xyzLevel3 = xyzLevels.get(2);
        Assert.assertEquals(1, xyzLevel1.getMutators().size());
        Assert.assertEquals(1, xyzLevel2.getMutators().size());
        Assert.assertEquals(1, xyzLevel3.getMutators().size());
        final AtlasChangeGenerator mutator1 = xyzLevel1.getMutators().iterator().next();
        Assert.assertEquals(AtlasChangeGeneratorSplitRoundabout.class.getSimpleName(),
                mutator1.getName());
        // 2 and 3 order below should be alphabetical by mutator name, since they are initially on
        // the same level, but then split because of different dynamic atlas policies.
        final AtlasChangeGenerator mutator2 = xyzLevel2.getMutators().iterator().next();
        Assert.assertEquals(AtlasChangeGeneratorAddTurnRestrictions.class.getSimpleName(),
                mutator2.getName());
        final AtlasChangeGenerator mutator3 = xyzLevel3.getMutators().iterator().next();
        Assert.assertEquals(AtlasChangeGeneratorRemoveReverseEdges.class.getSimpleName(),
                mutator3.getName());
    }

    private Map<String, List<AtlasMutationLevel>> countryToMutationLevels(
            final String configurationFile)
    {
        final Configuration configuration = new StandardConfiguration(new InputStreamResource(
                () -> AtlasMutatorConfigurationTest.class.getResourceAsStream(configurationFile)));
        final AtlasMutatorConfiguration atlasMutatorConfiguration = new AtlasMutatorConfiguration(
                Sets.hashSet(ABC, XYZ), null, null, null, null, Maps.hashMap(), configuration, true,
                false, false);
        return atlasMutatorConfiguration.getCountryToMutationLevels();
    }

    private Map<String, List<AtlasMutationLevel>> countryToMutationLevelsExtended(
            final String configurationFile)
    {
        final Configuration configuration = new StandardConfiguration(new InputStreamResource(
                () -> AtlasMutatorConfigurationTest.class.getResourceAsStream(configurationFile)));
        final AtlasMutatorConfiguration atlasMutatorConfiguration = new AtlasMutatorConfiguration(
                Sets.hashSet(ABC, XYZ, DEF, GHI), null, null, null, null, Maps.hashMap(),
                configuration, true, false, false);
        return atlasMutatorConfiguration.getCountryToMutationLevels();
    }
}
