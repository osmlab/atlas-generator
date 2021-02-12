package org.openstreetmap.atlas.mutator.configuration.mutators.aql;

import java.util.List;

import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.QueryExecutor;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.AqlConfigInfo;

/**
 * @author Yazad Khambata
 */
public interface AqlSupportedAtlasChangeGenerator extends AtlasChangeGenerator
{
    List<AqlConfigInfo> getAqlConfigInfos();

    Authenticator getAuthenticator();

    QueryExecutor getQueryExecutor();
}
