package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;

/**
 * @author Yazad Khambata
 */
public class SecureAqlConfigInfo implements AqlConfigInfo
{
    private static final long serialVersionUID = 834193230487629948L;
    private final String embeddedAql;
    private final String embeddedAqlSignature;

    public static SecureAqlConfigInfoBuilder builder(final Authenticator authenticator)
    {
        return new SecureAqlConfigInfoBuilder(authenticator);
    }

    SecureAqlConfigInfo(final String embeddedAql, final String embeddedAqlSignature)
    {
        this.embeddedAql = embeddedAql;
        this.embeddedAqlSignature = embeddedAqlSignature;
    }

    @Override
    public boolean equals(final Object other)
    {
        return EqualsBuilder.reflectionEquals(this, other);
    }

    @Override
    public String getEmbeddedAql()
    {
        return this.embeddedAql;
    }

    @Override
    public String getEmbeddedAqlSignature()
    {
        return this.embeddedAqlSignature;
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString()
    {
        return "SecureAqlConfigInfo(embeddedAql=" + this.getEmbeddedAql()
                + ", embeddedAqlSignature=" + this.getEmbeddedAqlSignature() + ")";
    }

    void structuralValidation()
    {
        Validate.notEmpty(this.embeddedAql, "embeddedAql is EMPTY. %s.", this);
        Validate.notEmpty(this.embeddedAqlSignature, "embeddedAqlSignature is EMPTY. %s.", this);
    }

    void validateSignature(final Authenticator authenticator)
    {
        authenticator.verify(this.embeddedAql, this.embeddedAqlSignature);
    }
}
