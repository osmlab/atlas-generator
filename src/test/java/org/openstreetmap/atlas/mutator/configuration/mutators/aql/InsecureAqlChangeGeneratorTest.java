package org.openstreetmap.atlas.mutator.configuration.mutators.aql;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.AqlConfigInfo;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.AqlConfigInfoBuilderFactory;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yazad Khambata
 */
public class InsecureAqlChangeGeneratorTest
{

    private static final String QUERY_1 = "update atlas.node set node.addTag(hello: 'world') where node.hasId(1)";

    private static final Logger log = LoggerFactory.getLogger(InsecureAqlChangeGeneratorTest.class);

    @Rule
    public final AqlChangeGeneratorTestRule rule = new AqlChangeGeneratorTestRule();

    @Test
    public void testAdd()
    {
        final List<AqlConfigInfo> aqlConfigInfos = Arrays
                .asList(AqlConfigInfoBuilderFactory.insecure().embeddedAql(QUERY_1).build());

        final Set<FeatureChange> featureChanges = runChangeGenerator(aqlConfigInfos);

        Assert.assertEquals(1, featureChanges.size());
        featureChanges.stream().forEach(featureChange -> Assert
                .assertTrue(featureChange.getAfterView().getTags().containsKey("hello")));
    }

    private Set<FeatureChange> runChangeGenerator(final List<AqlConfigInfo> aqlConfigInfos)
    {
        log.info("atlas: {}.", this.rule.getAtlas());

        final Configuration configuration = new StandardConfiguration(new StringResource(
                "{'InsecureAqlChangeGenerator': " + "{'enabled': true, 'countries': ['HWT']} }"));

        final InsecureAqlChangeGenerator insecureAqlChangeGenerator = new InsecureAqlChangeGenerator(
                InsecureAqlChangeGenerator.class.getSimpleName(), configuration, aqlConfigInfos);

        final Set<FeatureChange> featureChanges = insecureAqlChangeGenerator
                .generateWithoutValidation(this.rule.getAtlas());
        log.info("featureChanges: {}.", featureChanges);

        return featureChanges;
    }
}
