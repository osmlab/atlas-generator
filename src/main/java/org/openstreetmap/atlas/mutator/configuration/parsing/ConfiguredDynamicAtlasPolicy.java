package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.dynamic.policy.DynamicAtlasPolicy;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.InputDependency;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;
import org.openstreetmap.atlas.utilities.configuration.ConfiguredFilter;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.maps.MultiMapWithSet;
import org.openstreetmap.atlas.utilities.scalars.Distance;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

/**
 * @author matthieun
 */
public final class ConfiguredDynamicAtlasPolicy implements Serializable
{
    public static final ConfiguredDynamicAtlasPolicy DEFAULT = new ConfiguredDynamicAtlasPolicy();
    public static final String DEFAULT_NAME = "default";

    public static final String TYPE_JSON_PROPERTY_VALUE = "_dynamicAtlasPolicy";
    public static final String NAME_JSON_PROPERTY = "name";
    public static final String EXTEND_INDEFINITELY_JSON_PROPERTY = "extendIndefinitely";
    public static final String DEFER_LOADING_JSON_PROPERTY = "deferLoading";
    public static final String AGGRESSIVELY_EXPLORE_RELATIONS_JSON_PROPERTY = "aggressivelyExploreRelations";
    public static final String ENTITIES_TO_CONSIDER_FOR_EXPANSION_JSON_PROPERTY = "entitiesToConsiderForExpansion";
    public static final String MAX_EXPANSION_DISTANCE_METERS_JSON_PROPERTY = "maximumExpansionDistanceInMeters";
    public static final String FETCHER_JSON_PROPERTY = "fetcher";
    public static final String DIRECT_FETCHER_JSON_PROPERTY = "directFetcher";
    public static final String INPUT_DEPENDENCY_JSON_PROPERTY = "inputDependency";
    public static final String IS_PRE_DETERMINED_JSON_PROPERTY = "isPreDetermined";

    private static final long serialVersionUID = 3221637872080329610L;
    private static final Logger logger = LoggerFactory
            .getLogger(ConfiguredDynamicAtlasPolicy.class);
    private static final String CONFIGURATION_ROOT = AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL
            + ".dynamicAtlasPolicies";
    private static final String CONFIGURATION_EXTEND_INDEFINITELY = "extendIndefinitely";
    private static final String CONFIGURATION_DEFER_LOADING = "deferLoading";
    private static final String CONFIGURATION_AGGRESSIVELY_EXPLORE_RELATIONS = "aggressivelyExploreRelations";
    private static final String CONFIGURATION_DIRECT_FETCHER = "directFetcher";
    private static final String CONFIGURATION_FETCHER = "fetcher";
    private static final String CONFIGURATION_ENTITIES_TO_CONSIDER_FOR_EXPANSION = "entitiesToConsiderForExpansion";
    private static final String CONFIGURATION_MAXIMUM_EXPANSION_DISTANCE = "maximumExpansionDistanceInMeters";
    private static final String CONFIGURATION_MAXIMUM_EXPANSION_DISTANCE_DEFAULT = "N/A";

    private final String name;
    private final boolean extendIndefinitely;
    private final boolean deferLoading;
    private final boolean aggressivelyExploreRelations;
    private final String maximumExpansionDistanceInMeters;
    private final ConfiguredFilter entitiesToConsiderForExpansion;
    private final ConfiguredAtlasFetcher directFetcher;
    private final ConfiguredAtlasFetcher fetcher;
    private final InputDependency inputDependency;

    private transient MultiMapWithSet<Shard, String> shardsToCountries;

    public static ConfiguredDynamicAtlasPolicy from(final String name,
            final Configuration configuration)
    {
        if (DEFAULT_NAME.equals(name))
        {
            return DEFAULT;
        }
        if (!new ConfigurationReader(CONFIGURATION_ROOT).isPresent(configuration, name))
        {
            logger.warn(
                    "Attempted to create ConfiguredDynamicAtlasPolicy called \"{}\" but it was not found. It will be swapped with default policy.",
                    name);
            return DEFAULT;
        }
        return new ConfiguredDynamicAtlasPolicy(name, configuration);
    }

    private ConfiguredDynamicAtlasPolicy()
    {
        this(DEFAULT_NAME, new StandardConfiguration(new StringResource("{}")));
    }

    private ConfiguredDynamicAtlasPolicy(final String name, final Configuration configuration)
    {
        this.name = name;
        try
        {
            final String root = CONFIGURATION_ROOT + "." + name;
            final ConfigurationReader reader = new ConfigurationReader(root);
            this.extendIndefinitely = readBoolean(configuration, reader,
                    CONFIGURATION_EXTEND_INDEFINITELY, false);
            this.deferLoading = readBoolean(configuration, reader, CONFIGURATION_DEFER_LOADING,
                    true);
            this.aggressivelyExploreRelations = readBoolean(configuration, reader,
                    CONFIGURATION_AGGRESSIVELY_EXPLORE_RELATIONS, false);
            this.maximumExpansionDistanceInMeters = reader.configurationValue(configuration,
                    CONFIGURATION_MAXIMUM_EXPANSION_DISTANCE,
                    CONFIGURATION_MAXIMUM_EXPANSION_DISTANCE_DEFAULT);
            this.entitiesToConsiderForExpansion = ConfiguredFilter.from(reader.configurationValue(
                    configuration, CONFIGURATION_ENTITIES_TO_CONSIDER_FOR_EXPANSION,
                    ConfiguredFilter.DEFAULT), configuration);
            this.directFetcher = ConfiguredAtlasFetcher
                    .from(reader.configurationValue(configuration, CONFIGURATION_DIRECT_FETCHER,
                            ConfiguredAtlasFetcher.DEFAULT), configuration);
            this.fetcher = ConfiguredAtlasFetcher.from(reader.configurationValue(configuration,
                    CONFIGURATION_FETCHER, ConfiguredAtlasFetcher.DEFAULT), configuration);
            this.inputDependency = this.fetcher.getInputDependencyName()
                    .map(inputDependencyName -> new InputDependency(null, inputDependencyName,
                            configuration))
                    .orElse(null);
            this.shardsToCountries = new MultiMapWithSet<>();
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to create ConfiguredDynamicAtlasPolicy \"{}\"", name,
                    e);
        }
    }

    @Override
    public boolean equals(final Object other)
    {
        if (other instanceof ConfiguredDynamicAtlasPolicy)
        {
            return this.getName().equals(((ConfiguredDynamicAtlasPolicy) other).getName());
        }
        return false;
    }

    public ConfiguredAtlasFetcher getConfiguredFetcher()
    {
        return this.fetcher;
    }

    public ConfiguredFilter getEntitiesToConsiderForExpansion()
    {
        return this.entitiesToConsiderForExpansion;
    }

    public Optional<InputDependency> getInputDependency()
    {
        return Optional.ofNullable(this.inputDependency);
    }

    public Optional<String> getInputDependencyName()
    {
        return this.fetcher.getInputDependencyName();
    }

    public String getName()
    {
        return this.name;
    }

    public DynamicAtlasPolicy getPolicy(final Set<Shard> initialShards, final Sharding sharding,
            final String atlasPath, final Map<String, String> sparkConfiguration,
            final String country)
    {
        return getPolicy(initialShards, sharding,
                getFetcher(initialShards, atlasPath, country, sparkConfiguration));
    }

    public DynamicAtlasPolicy getRDDBasedPolicy(final Set<Shard> initialShards,
            final Sharding sharding, final Map<Shard, PackedAtlas> shardToAtlasMap)
    {
        return getPolicy(initialShards, sharding,
                shard -> Optional.ofNullable(shardToAtlasMap.get(shard)));
    }

    public Function<Shard, Set<Shard>> getShardExplorer(final Sharding sharding)
    {
        if (!isPreDetermined())
        {
            throw new CoreException("Impossible to know what shards to request in advance for {}",
                    this.name);
        }
        if (this.entitiesToConsiderForExpansion.isNoExpansion())
        {
            // Here: Nosonar: This needs to be serializable, and sonar flags it.
            return (Serializable & Function<Shard, Set<Shard>>) Sets::hashSet; // NOSONAR
        }
        return (Serializable & Function<Shard, Set<Shard>>) shard ->
        {
            final Rectangle maximumBounds = computeMaximumBounds(Sets.hashSet(shard));
            final Set<Shard> result = Iterables.asSet(sharding.shards(maximumBounds));
            result.add(shard);
            return result;
        };
    }

    @Override
    public int hashCode()
    {
        return getName().hashCode();
    }

    /**
     * @return Whether this policy is pre-determined, meaning that it is possible to know in advance
     *         what atlas files would be loaded for a given shard.
     */
    public boolean isPreDetermined()
    {
        return
        // We do not load neighboring shards for matching relations
        !this.aggressivelyExploreRelations
                // AND
                && (

                // There is a maximum expansion distance
                !CONFIGURATION_MAXIMUM_EXPANSION_DISTANCE_DEFAULT
                        .equals(this.maximumExpansionDistanceInMeters)

                        // OR Never expand
                        || this.entitiesToConsiderForExpansion.isNoExpansion()

                );
    }

    public JsonObject toJson()
    {
        final JsonObject policyObject = new JsonObject();
        policyObject.addProperty(AtlasMutatorConfiguration.TYPE_JSON_PROPERTY,
                TYPE_JSON_PROPERTY_VALUE);
        policyObject.addProperty(NAME_JSON_PROPERTY, this.name);
        policyObject.addProperty(EXTEND_INDEFINITELY_JSON_PROPERTY, this.extendIndefinitely);
        policyObject.addProperty(DEFER_LOADING_JSON_PROPERTY, this.deferLoading);
        policyObject.addProperty(AGGRESSIVELY_EXPLORE_RELATIONS_JSON_PROPERTY,
                this.aggressivelyExploreRelations);
        policyObject.addProperty(ENTITIES_TO_CONSIDER_FOR_EXPANSION_JSON_PROPERTY,
                this.entitiesToConsiderForExpansion.toString());
        policyObject.addProperty(MAX_EXPANSION_DISTANCE_METERS_JSON_PROPERTY,
                this.maximumExpansionDistanceInMeters);
        policyObject.addProperty(DIRECT_FETCHER_JSON_PROPERTY, this.directFetcher.toString());
        policyObject.addProperty(FETCHER_JSON_PROPERTY, this.fetcher.toString());
        policyObject.addProperty(IS_PRE_DETERMINED_JSON_PROPERTY, this.isPreDetermined());
        if (this.inputDependency != null)
        {
            policyObject.addProperty(INPUT_DEPENDENCY_JSON_PROPERTY,
                    this.inputDependency.getPathName());
        }
        return policyObject;
    }

    @Override
    public String toString()
    {
        return "ConfiguredDynamicAtlasPolicy [name=" + this.name + ", extendIndefinitely="
                + this.extendIndefinitely + ", deferLoading=" + this.deferLoading
                + ", aggressivelyExploreRelations=" + this.aggressivelyExploreRelations
                + ", entitiesToConsiderForExpansion=" + this.entitiesToConsiderForExpansion
                + ", fetcher=" + this.fetcher + "]";
    }

    public String toStringCompact()
    {
        return this.name;
    }

    public ConfiguredDynamicAtlasPolicy withShardsToCountries(
            final MultiMapWithSet<Shard, String> shardsToCountries)
    {
        this.shardsToCountries = shardsToCountries;
        return this;
    }

    private Rectangle computeMaximumBounds(final Set<Shard> initialShards)
    {
        final Rectangle maximumBounds;
        if (CONFIGURATION_MAXIMUM_EXPANSION_DISTANCE_DEFAULT
                .equals(this.maximumExpansionDistanceInMeters))
        {
            maximumBounds = Rectangle.MAXIMUM;
        }
        else
        {
            final Distance expansionDistance = Distance
                    .meters(Double.valueOf(this.maximumExpansionDistanceInMeters));
            Rectangle start = null;
            for (final Shard initialShard : initialShards)
            {
                if (start == null)
                {
                    start = initialShard.bounds();
                }
                else
                {
                    start = start.combine(initialShard.bounds());
                }
            }
            if (start == null)
            {
                throw new CoreException("There should be at least one shard to start with.");
            }
            maximumBounds = start.expand(expansionDistance);
        }
        return maximumBounds;
    }

    private Function<Shard, Optional<Atlas>> getFetcher(final Set<Shard> initialShards,
            final String atlasPath, final String country,
            final Map<String, String> sparkConfiguration)
    {
        final Function<Shard, Optional<Atlas>> initialShardAtlasFetcher = this.directFetcher
                .withShardsToCountries(this.shardsToCountries)
                .getFetcher(atlasPath, country, sparkConfiguration);
        final Function<Shard, Optional<Atlas>> subAtlasFetcher = this.fetcher
                .withShardsToCountries(this.shardsToCountries)
                .getFetcher(atlasPath, country, sparkConfiguration);
        return shardSource ->
        {
            final Time start = Time.now();
            final Optional<Atlas> result;
            final String initialShard;
            // Shard aware: return the full Atlas for the initial shards
            if (initialShards.contains(shardSource))
            {
                result = initialShardAtlasFetcher.apply(shardSource);
                initialShard = "(initial shard)";
            }
            else
            {
                result = subAtlasFetcher.apply(shardSource);
                initialShard = "(satellite shard)";
            }
            logger.debug("Fetched {} and {} and {} {} in {}", atlasPath, country,
                    shardSource.getName(), initialShard, start.elapsedSince());
            return result;
        };
    }

    private DynamicAtlasPolicy getPolicy(final Set<Shard> initialShards, final Sharding sharding,
            final Function<Shard, Optional<Atlas>> fetcher)
    {
        final Rectangle maximumBounds = computeMaximumBounds(initialShards);
        return new DynamicAtlasPolicy(fetcher, sharding, initialShards, maximumBounds)
                .withExtendIndefinitely(this.extendIndefinitely)
                .withDeferredLoading(this.deferLoading)
                .withAggressivelyExploreRelations(this.aggressivelyExploreRelations)
                .withAtlasEntitiesToConsiderForExpansion(this.entitiesToConsiderForExpansion);
    }

    private boolean readBoolean(final Configuration configuration, final ConfigurationReader reader,
            final String booleanName, final boolean defaultValue)
    {
        try
        {
            return reader.configurationValue(configuration, booleanName, defaultValue);
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to read \"{}\"", booleanName, e);
        }
    }
}
