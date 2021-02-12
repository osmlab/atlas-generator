package org.openstreetmap.atlas.mutator.configuration.parsing.provider.file;

import java.util.Map;
import java.util.Optional;

import org.openstreetmap.atlas.generator.PbfLocator;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.raw.creation.RawAtlasGenerator;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.tags.Taggable;
import org.openstreetmap.atlas.tags.filters.ConfiguredTaggableFilter;
import org.openstreetmap.atlas.tags.filters.matcher.TaggableMatcher;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide an Atlas when given a PBF resource. Use the provided configuration to determine what to
 * bring in.
 * 
 * @author matthieun
 */
public class PbfRawAtlasProvider extends AbstractFileAtlasProvider
{
    private static final Logger logger = LoggerFactory.getLogger(PbfRawAtlasProvider.class);

    private static final String CONFIGURATION_PBF_NODE = "pbfNode";
    private static final String CONFIGURATION_PBF_WAY = "pbfWay";
    private static final String CONFIGURATION_PBF_RELATION = "pbfRelation";
    private static final String CONFIGURATION_PBF_SCHEME = "pbfScheme";
    private static final long serialVersionUID = 219796406525700034L;

    private final TaggableMatcher pbfNodeMatcher;
    private final TaggableMatcher pbfWayMatcher;
    private final TaggableMatcher pbfRelationMatcher;
    private final SlippyTilePersistenceScheme pbfScheme;

    // This cache can be transient since it is configured on the executor, right when the
    // ConfiguredAtlasFetcher generates the custom Fetcher function to be used by the
    // DynamicAtlasPolicy
    private transient HadoopPbfFileCache cache;

    public PbfRawAtlasProvider(final Configuration configuration)
    {
        final String root = "";
        final ConfigurationReader reader = new ConfigurationReader(root);
        final String pbfNode = reader.configurationValue(configuration, CONFIGURATION_PBF_NODE, "");
        final String pbfWay = reader.configurationValue(configuration, CONFIGURATION_PBF_WAY, "");
        final String pbfRelation = reader.configurationValue(configuration,
                CONFIGURATION_PBF_RELATION, "");
        final String pbfSchemeString = reader.configurationValue(configuration,
                CONFIGURATION_PBF_SCHEME, PbfLocator.DEFAULT_SCHEME);
        this.pbfNodeMatcher = TaggableMatcher.from(pbfNode);
        this.pbfWayMatcher = TaggableMatcher.from(pbfWay);
        this.pbfRelationMatcher = TaggableMatcher.from(pbfRelation);
        this.pbfScheme = SlippyTilePersistenceScheme.getSchemeInstanceFromString(pbfSchemeString);
    }

    @Override
    public void setAtlasProviderContext(final Map<String, Object> context)
    {
        super.setAtlasProviderContext(context);
        this.cache = new HadoopPbfFileCache(getAtlasPath(), this.pbfScheme,
                getSparkConfiguration());
    }

    @Override
    protected Optional<Resource> getResourceFromCache(final String country, final Shard shard)
    {
        return this.cache.get(shard);
    }

    @Override
    protected void invalidateCache(final String country, final Shard shard)
    {
        this.cache.invalidate(shard);
    }

    @Override
    protected Optional<Atlas> resourceToAtlas(final Resource resource, final String country,
            final Shard shard)
    {
        final AtlasLoadingOption atlasLoadingOption = AtlasLoadingOption
                .createOptionWithNoSlicing();

        // Set the country code that is being processed!
        atlasLoadingOption.setCountryCode(country);
        final Configuration emptyConfiguration = new StandardConfiguration(
                new StringResource("{filters:[]}"));
        atlasLoadingOption.setOsmPbfNodeFilter(new ConfiguredTaggableFilter(emptyConfiguration)
        {
            private static final long serialVersionUID = 4881464932217937980L;

            @Override
            public boolean test(final Taggable taggable)
            {
                return PbfRawAtlasProvider.this.pbfNodeMatcher.test(taggable);
            }
        });
        atlasLoadingOption.setOsmPbfWayFilter(new ConfiguredTaggableFilter(emptyConfiguration)
        {
            private static final long serialVersionUID = -3018918280683785215L;

            @Override
            public boolean test(final Taggable taggable)
            {
                return PbfRawAtlasProvider.this.pbfWayMatcher.test(taggable);
            }
        });
        atlasLoadingOption.setOsmPbfRelationFilter(new ConfiguredTaggableFilter(emptyConfiguration)
        {
            private static final long serialVersionUID = -4496334067178504004L;

            @Override
            public boolean test(final Taggable taggable)
            {
                return PbfRawAtlasProvider.this.pbfRelationMatcher.test(taggable);
            }
        });

        logger.debug("Loading PBF to Atlas from Resource {}", resource.getName());
        final Time start = Time.now();
        final Atlas result = new RawAtlasGenerator(resource, atlasLoadingOption,
                MultiPolygon.forPolygon(shard.bounds())).build();
        logger.debug("Loaded PBF to Atlas from Resource {} in {}. Atlas result: {}",
                resource.getName(), start.elapsedSince(), result);
        return Optional.ofNullable(result);
    }
}
