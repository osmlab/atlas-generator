package org.openstreetmap.atlas.mutator.configuration.mutators.aql;

import java.util.List;

import org.apache.commons.lang3.Validate;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.impl.SHA512HMACAuthenticatorImpl;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.QueryExecutor;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.impl.SecureQueryExecutorImpl;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.AqlConfigInfo;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.SecureAqlConfigInfo;
import org.openstreetmap.atlas.mutator.configuration.util.consts.AqlConstants;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * An {@link org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator} that runs AQL
 * using a {@link SecureQueryExecutorImpl} to securely executeAql signed AQL queries.
 *
 * @author Yazad Khambata
 */
public class SecureAqlChangeGenerator extends AbstractAqlChangeGenerator
{
    public SecureAqlChangeGenerator(final String name, final Configuration configuration)
    {
        super(name, configuration);
    }

    public SecureAqlChangeGenerator(final String name, final Configuration configuration,
            final List<AqlConfigInfo> aqlConfigInfos)
    {
        super(name, configuration, aqlConfigInfos);
    }

    @Override
    public Authenticator getAuthenticator()
    {
        final String secret = System.getProperty(AqlConstants.SYSTEM_KEY);
        Validate.notEmpty(secret, "The secret is empty. Please ste a system parameter [%s].",
                AqlConstants.SYSTEM_KEY);
        return new SHA512HMACAuthenticatorImpl(secret);
    }

    @Override
    public QueryExecutor getQueryExecutor()
    {
        return new SecureQueryExecutorImpl();
    }

    @Override
    protected AqlConfigInfo toAqlConfigInfo(final Authenticator authenticator, final String query,
            final String queryClasspath, final String signature)
    {
        return SecureAqlConfigInfo.builder(authenticator).externalAqlClasspath(queryClasspath)
                .embeddedAql(query).embeddedAqlSignature(signature).build();
    }
}
