package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

/**
 * @author Yazad Khambata
 */
public class InsecureAqlConfigInfoBuilder
        extends AbstractAqlConfigBuilder<InsecureAqlConfigInfoBuilder>
{
    InsecureAqlConfigInfoBuilder()
    {
    }

    public InsecureAqlConfigInfo build()
    {
        return new InsecureAqlConfigInfo(getEmbeddedAql());
    }
}
