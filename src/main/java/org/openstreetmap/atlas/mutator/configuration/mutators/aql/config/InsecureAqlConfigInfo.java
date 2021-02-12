package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * @author Yazad Khambata
 */
public class InsecureAqlConfigInfo implements AqlConfigInfo
{
    private static final String EMBEDDED_AQL_SIGNATURE = "NA";
    private static final long serialVersionUID = -7284523951560601482L;
    private final String embeddedAql;

    public static InsecureAqlConfigInfoBuilder builder()
    {
        return new InsecureAqlConfigInfoBuilder();
    }

    InsecureAqlConfigInfo(final String embeddedAql)
    {
        super();
        this.embeddedAql = embeddedAql;
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
        return InsecureAqlConfigInfo.EMBEDDED_AQL_SIGNATURE;
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString()
    {
        return "InsecureAqlConfigInfo(embeddedAql=" + this.getEmbeddedAql() + ")";
    }
}
