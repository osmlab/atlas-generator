package org.openstreetmap.atlas.mutator.configuration.mutators.aql;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.impl.SHA512HMACAuthenticatorImpl;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.AqlConfigInfo;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.SecureAqlConfigInfo;
import org.openstreetmap.atlas.mutator.configuration.util.consts.AqlConstants;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yazad Khambata
 */
public class SecureAqlChangeGeneratorTest
{
    private static final Logger log = LoggerFactory.getLogger(SecureAqlChangeGeneratorTest.class);
    /**
     * Add a tag.
     */
    private static final String QUERY_1 = "update atlas.node set node.addTag(aparecium: 'tag') where node.hasIds(1, 2)";
    /**
     * Delete a tag.
     */
    private static final String QUERY_2 = "update atlas.node set node.deleteTag('obliviate') where node.hasIds(1, 2)";
    /**
     * Replace a tag.
     */
    private static final String QUERY_3 = "update atlas.node set node.addTag(shapeshift: 'animagus') where node.hasIds(1)";
    private static final String secret = UUID.randomUUID().toString();
    @Rule
    public final AqlChangeGeneratorTestRule rule = new AqlChangeGeneratorTestRule();

    @Test
    public void testAdd()
    {
        final Authenticator authenticator = getAuthenticator();
        final List<AqlConfigInfo> aqlConfigInfos = Arrays
                .asList(toAqlConfigInfo(authenticator, QUERY_1));

        final Set<FeatureChange> featureChanges = runChangeGenerator(aqlConfigInfos);

        Assert.assertEquals(2, featureChanges.size());
        featureChanges.stream().forEach(featureChange -> Assert
                .assertTrue(featureChange.getAfterView().getTags().containsKey("aparecium")));
    }

    @Test
    public void testDelete()
    {
        final Atlas atlas = this.rule.getAtlas();

        Assert.assertEquals(2, atlas.numberOfNodes());
        Assert.assertEquals("memory", atlas.node(1).tag("obliviate"));

        final Authenticator authenticator = getAuthenticator();
        final List<AqlConfigInfo> aqlConfigInfos = Arrays
                .asList(toAqlConfigInfo(authenticator, QUERY_2));

        final Set<FeatureChange> featureChanges = runChangeGenerator(aqlConfigInfos);

        Assert.assertEquals(2, featureChanges.size());
        featureChanges.stream().forEach(featureChange -> Assert
                .assertFalse(featureChange.getAfterView().getTags().containsKey("obliviate")));
    }

    @Test
    public void testReplace()
    {
        final Authenticator authenticator = getAuthenticator();
        final List<AqlConfigInfo> aqlConfigInfos = Arrays
                .asList(toAqlConfigInfo(authenticator, QUERY_3));

        Assert.assertEquals("nagini", this.rule.getAtlas().node(1).tag("shapeshift"));

        final Set<FeatureChange> featureChanges = runChangeGenerator(aqlConfigInfos);

        Assert.assertEquals(1, featureChanges.size());
        Assert.assertEquals("animagus",
                featureChanges.iterator().next().getAfterView().tag("shapeshift"));
    }

    private Authenticator getAuthenticator()
    {
        System.setProperty(AqlConstants.SYSTEM_KEY, secret);
        return new SHA512HMACAuthenticatorImpl(secret);
    }

    private Set<FeatureChange> runChangeGenerator(final List<AqlConfigInfo> aqlConfigInfos)
    {
        log.info("atlas: {}.", this.rule.getAtlas());

        final Configuration configuration = new StandardConfiguration(new StringResource(
                "{'SecureAqlChangeGenerator': " + "{'enabled': true, 'countries': ['HWT']} }"));

        final SecureAqlChangeGenerator secureAqlChangeGenerator = new SecureAqlChangeGenerator(
                SecureAqlChangeGenerator.class.getSimpleName(), configuration, aqlConfigInfos);

        final Set<FeatureChange> featureChanges = secureAqlChangeGenerator
                .generateWithoutValidation(this.rule.getAtlas());
        log.info("featureChanges: {}.", featureChanges);

        return featureChanges;
    }

    private SecureAqlConfigInfo toAqlConfigInfo(final Authenticator authenticator,
            final String query)
    {
        return SecureAqlConfigInfo.builder(authenticator).embeddedAql(query)
                .embeddedAqlSignature(authenticator.sign(query)).build();
    }
}
