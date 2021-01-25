package org.openstreetmap.atlas.mutator.configuration.mutators.aql;

import java.util.List;

import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.QueryExecutor;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.impl.InsecureQueryExecutorImpl;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.impl.SecureQueryExecutorImpl;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.AqlConfigInfo;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.InsecureAqlConfigInfo;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * An {@link org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator} that runs AQL
 * using a {@link SecureQueryExecutorImpl} to securely executeAql signed AQL queries.
 *
 * @author Yazad Khambata
 */
public class InsecureAqlChangeGenerator extends AbstractAqlChangeGenerator
{
    public InsecureAqlChangeGenerator(final String name, final Configuration configuration)
    {
        super(name, configuration);
    }

    public InsecureAqlChangeGenerator(final String name, final Configuration configuration,
            final List<AqlConfigInfo> aqlConfigInfos)
    {
        super(name, configuration, aqlConfigInfos);
    }

    @Override
    public Authenticator getAuthenticator()
    {
        return new Authenticator()
        {
            @Override
            public String sign(final String message)
            {
                return "NA";
            }

            @Override
            public void verify(final String message, final String signature)
            {
                // Insure never verifies.
            }
        };
    }

    @Override
    public QueryExecutor getQueryExecutor()
    {
        return new InsecureQueryExecutorImpl();
    }

    @Override
    protected AqlConfigInfo toAqlConfigInfo(final Authenticator authenticator, final String query,
            final String queryClasspath, final String signature)
    {
        return InsecureAqlConfigInfo.builder().externalAqlClasspath(queryClasspath)
                .embeddedAql(query).embeddedAqlSignature(signature).build();
    }
}
