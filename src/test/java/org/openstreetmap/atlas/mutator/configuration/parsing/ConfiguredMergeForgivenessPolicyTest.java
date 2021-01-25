package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.change.MergeFailureType;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.ConfiguredMergeForgivenessPolicy;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.MergeForgivenessStrategy;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lcram
 */
public class ConfiguredMergeForgivenessPolicyTest
{
    /**
     * A dummy strategy for this test.
     * 
     * @author lcram
     */
    public static class TestStrategy implements MergeForgivenessStrategy
    {
        private static final long serialVersionUID = 3820990226789993358L;

        @Override
        public FeatureChange resolve(final FeatureChange left, final FeatureChange right,
                final Map<String, Serializable> configuration)
        {
            return left;
        }
    }

    private static final Logger logger = LoggerFactory
            .getLogger(ConfiguredMergeForgivenessPolicyTest.class);

    @Test
    public void testMergeForgivenessPolicy()
    {
        final Configuration configuration = new StandardConfiguration(new InputStreamResource(
                () -> ConfiguredMergeForgivenessPolicyTest.class.getResourceAsStream(
                        ConfiguredMergeForgivenessPolicyTest.class.getSimpleName() + ".json")));

        final ConfiguredMergeForgivenessPolicy policy = ConfiguredMergeForgivenessPolicy
                .fromGlobal(configuration);

        final ConfiguredMergeForgivenessPolicy.RootLevelPolicyElement rootLevelElement = policy
                .getRootLevelFailurePolicy().get(0);
        Assert.assertEquals(MergeFailureType.FEATURE_CHANGE_INVALID_ADD_REMOVE_MERGE,
                rootLevelElement.getRootLevelFailureType());
        Assert.assertEquals(TestStrategy.class.getName(),
                rootLevelElement.getStrategy().getClass().getName());
        Assert.assertEquals("bar", rootLevelElement.getConfiguration().get("foo"));

        final ConfiguredMergeForgivenessPolicy.FailureSequencePolicyElement subSequencePolicyElement = policy
                .getSubSequenceFailurePolicy().get(0);
        Assert.assertEquals(
                Arrays.asList(MergeFailureType.AUTOFAIL_POLYLINE_MERGE,
                        MergeFailureType.BEFORE_VIEW_MERGE_STRATEGY_FAILED),
                subSequencePolicyElement.getFailureSequence());
        Assert.assertEquals(TestStrategy.class.getName(),
                subSequencePolicyElement.getStrategy().getClass().getName());
        Assert.assertTrue(subSequencePolicyElement.getConfiguration().isEmpty());

        final ConfiguredMergeForgivenessPolicy.FailureSequencePolicyElement exactSequencePolicyElement = policy
                .getExactSequenceFailurePolicy().get(0);
        Assert.assertEquals(
                Arrays.asList(MergeFailureType.AUTOFAIL_POLYLINE_MERGE,
                        MergeFailureType.BEFORE_VIEW_MERGE_STRATEGY_FAILED,
                        MergeFailureType.HIGHEST_LEVEL_MERGE_FAILURE),
                exactSequencePolicyElement.getFailureSequence());
        Assert.assertEquals(TestStrategy.class.getName(),
                exactSequencePolicyElement.getStrategy().getClass().getName());
        Assert.assertEquals(Maps.hashMap("foo", "bar"),
                exactSequencePolicyElement.getConfiguration().get("baz"));
    }
}
