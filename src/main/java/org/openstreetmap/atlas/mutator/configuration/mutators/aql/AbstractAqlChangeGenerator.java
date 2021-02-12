package org.openstreetmap.atlas.mutator.configuration.mutators.aql;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.Authenticator;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.QueryExecutor;
import org.openstreetmap.atlas.geography.atlas.dsl.query.result.MutantResult;
import org.openstreetmap.atlas.geography.atlas.dsl.query.result.Result;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.mutators.aql.config.AqlConfigInfo;
import org.openstreetmap.atlas.utilities.configuration.Configurable;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yazad Khambata
 */
public abstract class AbstractAqlChangeGenerator extends ConfiguredAtlasChangeGenerator
        implements AqlSupportedAtlasChangeGenerator
{
    private static final Logger log = LoggerFactory.getLogger(AbstractAqlChangeGenerator.class);
    private static final long serialVersionUID = 8363380396415344618L;

    private final List<AqlConfigInfo> aqlConfigInfos;

    public AbstractAqlChangeGenerator(final String name, final Configuration configuration)
    {
        super(name, configuration);
        this.aqlConfigInfos = loadAqlConfig(getAuthenticator(), name, configuration);
    }

    public AbstractAqlChangeGenerator(final String name, final Configuration configuration,
            final List<AqlConfigInfo> aqlConfigInfos)
    {
        super(name, configuration);
        this.aqlConfigInfos = aqlConfigInfos;
    }

    @Override
    public boolean equals(final Object other)
    {
        return EqualsBuilder.reflectionEquals(this, other);
    }

    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        final QueryExecutor queryExecutor = getQueryExecutor();
        Validate.notEmpty(this.aqlConfigInfos, "aqlConfigInfos is EMPTY!");

        return this.aqlConfigInfos.stream()
                .map(aqlConfigInfo -> executeAql(atlas, queryExecutor, aqlConfigInfo))
                .map(result -> (MutantResult) result).map(MutantResult::getChange)
                .flatMap(Change::changes).collect(Collectors.toSet());
    }

    @Override
    public List<AqlConfigInfo> getAqlConfigInfos()
    {
        return Collections.unmodifiableList(this.aqlConfigInfos);
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    public List<AqlConfigInfo> loadAqlConfig(final Authenticator authenticator, final String name,
            final Configuration configuration)
    {
        final Configurable configurable = configuration.get(name);
        return loadAqlConfig(authenticator, name, configurable);
    }

    protected boolean canEqual(final Object other)
    {
        return other instanceof AbstractAqlChangeGenerator;
    }

    protected abstract AqlConfigInfo toAqlConfigInfo(Authenticator authenticator, String query,
            String queryClasspath, String signature);

    private Result executeAql(final Atlas atlas, final QueryExecutor queryExecutor,
            final AqlConfigInfo aqlConfigInfo)
    {
        final String embeddedAql = aqlConfigInfo.getEmbeddedAql();
        final String embeddedAqlSignature = aqlConfigInfo.getEmbeddedAqlSignature();

        log.info("embeddedAql: {}; embeddedAqlSignature: {}.", embeddedAql, embeddedAqlSignature);

        return queryExecutor.exec(atlas, embeddedAql, embeddedAqlSignature);
    }

    private List<AqlConfigInfo> loadAqlConfig(final Authenticator authenticator, final String name,
            final Configurable configurable)
    {
        final Map<String, Object> configParams = configurable.value();
        log.info("loadAqlConfig: {}; class: {}.", configParams, configParams.getClass());

        final Map<String, Object> aqlSection = (Map<String, Object>) configParams.get("aql");
        Validate.notEmpty(aqlSection, "embeddedAql: missing or empty in %s.", name);

        final List<Map<String, Object>> queriesSection = (List<Map<String, Object>>) aqlSection
                .get("queries");
        Validate.notEmpty(aqlSection, "embeddedAql -> queries: missing or empty in %s.", name);

        return queriesSection.stream()
                .map(configMapSection -> toAqlConfigInfo(authenticator, configMapSection))
                .collect(Collectors.toList());
    }

    private AqlConfigInfo toAqlConfigInfo(final Authenticator authenticator,
            final Map<String, Object> configMap)
    {
        final String queryClasspath = (String) configMap.get("queryClasspath");
        final String query = (String) configMap.get("query");

        final String signature = (String) configMap.get("signature");

        return toAqlConfigInfo(authenticator, query, queryClasspath, signature);
    }
}
