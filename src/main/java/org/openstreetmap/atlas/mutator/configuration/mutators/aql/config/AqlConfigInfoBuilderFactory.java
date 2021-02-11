package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;

/**
 * @author Yazad Khambata
 */
public final class AqlConfigInfoBuilderFactory
{
    public static InsecureAqlConfigInfoBuilder insecure()
    {
        return InsecureAqlConfigInfo.builder();
    }

    public static SecureAqlConfigInfoBuilder secure(final Authenticator authenticator)
    {
        return SecureAqlConfigInfo.builder(authenticator);
    }

    private AqlConfigInfoBuilderFactory()
    {
    }
}
