package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * An abstract builder.
 *
 * @param <B>
 *            - The builder implementation.
 * @author Yazad Khambata
 */
public class AbstractAqlConfigBuilder<B extends AbstractAqlConfigBuilder<B>>
{
    private String embeddedAql;
    private String embeddedAqlSignature;

    public B embeddedAql(final String paramEmbeddedAql)
    {
        if (paramEmbeddedAql == null)
        {
            return self();
        }

        Validate.isTrue(StringUtils.isEmpty(this.embeddedAql),
                "Embedded AQL is already set: [%s]; while trying to set: [%s]", this.embeddedAql,
                paramEmbeddedAql);
        this.embeddedAql = paramEmbeddedAql;
        return self();
    }

    public B embeddedAqlSignature(final String paramEmbeddedAqlSignature)
    {
        if (paramEmbeddedAqlSignature != null)
        {
            this.embeddedAqlSignature = paramEmbeddedAqlSignature;
        }

        return self();
    }

    public B externalAqlClasspath(final String externalAqlClasspath)
    {

        if (externalAqlClasspath == null)
        {
            return self();
        }
        Validate.isTrue(StringUtils.isEmpty(this.embeddedAql),
                "Either embeddedAql: [%s] OR externalAqlClasspath: [%s] can be provided per query but not both.",
                this.embeddedAql, externalAqlClasspath);

        final String extractedEmbeddedAql = AqlConfigHelper
                .loadAqlFromClasspath(externalAqlClasspath);
        final String extractedEmbeddedAqlSignature = AqlConfigHelper
                .loadAqlSignatureFromClasspath(externalAqlClasspath);
        return this.embeddedAql(extractedEmbeddedAql)
                .embeddedAqlSignature(extractedEmbeddedAqlSignature);
    }

    public String getEmbeddedAql()
    {
        return this.embeddedAql;
    }

    public String getEmbeddedAqlSignature()
    {
        return this.embeddedAqlSignature;
    }

    private B self()
    {
        return (B) this;
    }
}
