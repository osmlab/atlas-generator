package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;

/**
 * @author Yazad Khambata
 */
public final class SecureAqlConfigInfoBuilder
        extends AbstractAqlConfigBuilder<SecureAqlConfigInfoBuilder>
{
    private final Authenticator authenticator;

    SecureAqlConfigInfoBuilder(final Authenticator authenticator)
    {
        super();
        this.authenticator = authenticator;
    }

    public SecureAqlConfigInfo build()
    {
        final SecureAqlConfigInfo aqlConfigInfo = new SecureAqlConfigInfo(getEmbeddedAql(),
                getEmbeddedAqlSignature());
        aqlConfigInfo.structuralValidation();
        aqlConfigInfo.validateSignature(this.authenticator);
        return aqlConfigInfo;
    }
}
