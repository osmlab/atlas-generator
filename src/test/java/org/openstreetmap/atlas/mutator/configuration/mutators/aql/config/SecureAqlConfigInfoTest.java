package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.impl.SHA512HMACAuthenticatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yazad Khambata
 */
public class SecureAqlConfigInfoTest
{
    private static final Logger log = LoggerFactory.getLogger(SecureAqlConfigInfoTest.class);
    private final Authenticator authenticator = new SHA512HMACAuthenticatorImpl("dummy_secret");

    @Test
    public void testInvalid1()
    {
        try
        {
            SecureAqlConfigInfo.builder(this.authenticator).embeddedAql(null)
                    .embeddedAqlSignature(null).externalAqlClasspath(null).build();
        }
        catch (final Exception e)
        {
            Assert.assertTrue(e.getMessage().contains("embeddedAql is EMPTY"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void testInvalid2()
    {
        try
        {
            final String embeddedAql = "update ...";
            SecureAqlConfigInfo.builder(this.authenticator).embeddedAql(embeddedAql)
                    .embeddedAqlSignature(null).externalAqlClasspath(null).build();
        }
        catch (final Exception e)
        {
            log.error("", e);
            Assert.assertTrue(e.getMessage().contains("embeddedAqlSignature is EMPTY"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void testInvalid3()
    {
        try
        {
            final String embeddedAql = "update ...";
            SecureAqlConfigInfo.builder(this.authenticator).embeddedAql(embeddedAql)
                    .embeddedAqlSignature("INVALID SIGNATURE!").externalAqlClasspath(null).build();
        }
        catch (final Exception e)
        {
            Assert.assertTrue(e.getMessage().contains("Signature Mismatch"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void testValid1()
    {
        final String embeddedAql = "update ...";
        final SecureAqlConfigInfo aqlConfigInfo1 = SecureAqlConfigInfo.builder(this.authenticator)
                .embeddedAql(embeddedAql).embeddedAqlSignature(this.authenticator.sign(embeddedAql))
                .externalAqlClasspath(null).build();

        final SecureAqlConfigInfo aqlConfigInfo2 = SecureAqlConfigInfo.builder(this.authenticator)
                .externalAqlClasspath(null).embeddedAql(embeddedAql)
                .embeddedAqlSignature(this.authenticator.sign(embeddedAql)).build();

        Assert.assertEquals(aqlConfigInfo1, aqlConfigInfo2);
    }

    @Test
    public void testValid2()
    {
        final SecureAqlConfigInfo aqlConfigInfo1 = SecureAqlConfigInfo.builder(this.authenticator)
                .embeddedAql(null).embeddedAqlSignature(null)
                .externalAqlClasspath("/aql-files/testUpdate.aql").build();
        final SecureAqlConfigInfo aqlConfigInfo2 = SecureAqlConfigInfo.builder(this.authenticator)
                .externalAqlClasspath("/aql-files/testUpdate.aql").embeddedAql(null)
                .embeddedAqlSignature(null).build();

        Assert.assertEquals(aqlConfigInfo1, aqlConfigInfo2);
    }
}
