package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.InputDependency;
import org.openstreetmap.atlas.mutator.configuration.parsing.provider.AtlasProvider;
import org.openstreetmap.atlas.mutator.configuration.parsing.provider.ConfiguredAtlasProvider;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;
import org.openstreetmap.atlas.utilities.maps.MultiMapWithSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

/**
 * Configurable object that can provide a fetcher, i.e. a function that, provided a {@link Shard},
 * can return an {@link Optional} of {@link Atlas}.
 * 
 * @author matthieun
 */
public final class ConfiguredAtlasFetcher implements Serializable
{
    public static final String DEFAULT = "default";
    public static final String TYPE_JSON_PROPERTY_VALUE = "_fetcher";
    public static final String NAME_JSON_PROPERTY = "name";
    public static final String INPUT_DEPENDENCY_NAME_JSON_PROPERTY = "inputDependencyName";
    public static final String INPUT_DEPENDENCY_PATH_JSON_PROPERTY = "inputDependencyPath";
    public static final String SUB_ATLAS_JSON_PROPERTY = "subAtlas";
    public static final String CONFIGURATION_ROOT = AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL
            + ".fetchers";
    private static final long serialVersionUID = 4533389855900131531L;
    private static final Logger logger = LoggerFactory.getLogger(ConfiguredAtlasFetcher.class);
    private static final String CONFIGURATION_INPUT_DEPENDENCY = "inputDependency";
    private static final String CONFIGURATION_SUBATLAS = "subAtlas";
    private static final String CONFIGURATION_ATLAS_PROVIDER = "atlasProvider";
    // Countryvore means a fetcher will explore all other countries (and multi-atlas them together
    // before returning) for any shard it wants to expand to. Defaults to false.
    private static final String CONFIGURATION_COUNTRYVORE = "countryvore";

    private final String name;
    private final String inputDependencyName;
    private final String inputDependencyPath;
    private final ConfiguredSubAtlas subAtlas;
    private final AtlasProvider atlasProvider;
    private final boolean countryvore;

    private transient MultiMapWithSet<Shard, String> shardsToCountries;

    public static ConfiguredAtlasFetcher direct()
    {
        return new ConfiguredAtlasFetcher();
    }

    public static ConfiguredAtlasFetcher from(final String name, final Configuration configuration)
    {
        if (DEFAULT.equals(name))
        {
            return direct();
        }
        if (!new ConfigurationReader(CONFIGURATION_ROOT).isPresent(configuration, name))
        {
            logger.warn(
                    "Attempted to create ConfiguredAtlasFetcher called \"{}\" but it was not found. It will be swapped with default passthrough fetcher.",
                    name);
            return direct();
        }
        return new ConfiguredAtlasFetcher(name, configuration);
    }

    private ConfiguredAtlasFetcher()
    {
        this.name = DEFAULT;
        this.inputDependencyName = "";
        this.inputDependencyPath = "";
        this.subAtlas = ConfiguredSubAtlas.UNCHANGED;
        this.atlasProvider = AtlasProvider.defaultProvider();
        this.countryvore = false;
    }

    private ConfiguredAtlasFetcher(final String name, final Configuration configuration)
    {
        this.name = name;
        final String root = CONFIGURATION_ROOT + "." + name;
        final ConfigurationReader reader = new ConfigurationReader(root);
        this.inputDependencyName = reader.configurationValue(configuration,
                CONFIGURATION_INPUT_DEPENDENCY, "");
        if (!this.inputDependencyName.isEmpty()
                && !new ConfigurationReader(InputDependency.CONFIGURATION_ROOT)
                        .isPresent(configuration, this.inputDependencyName))
        {
            throw new CoreException(
                    "InputDependency name \"{}\" in ConfiguredAtlasFetcher \"{}\" is missing from the input dependency list.",
                    this.inputDependencyName, name);
        }
        this.inputDependencyPath = this.inputDependencyName.isEmpty() ? ""
                : InputDependency.INPUT_DEPENDENCY_FOLDER_KEY + this.inputDependencyName;
        this.subAtlas = ConfiguredSubAtlas.from(reader.configurationValue(configuration,
                CONFIGURATION_SUBATLAS, ConfiguredSubAtlas.DEFAULT), configuration);
        final String atlasProviderName = reader.configurationValue(configuration,
                CONFIGURATION_ATLAS_PROVIDER, "");
        if (!atlasProviderName.isEmpty())
        {
            this.atlasProvider = new ConfiguredAtlasProvider(atlasProviderName, configuration)
                    .getAtlasProvider();
        }
        else
        {
            this.atlasProvider = AtlasProvider.defaultProvider();
        }
        this.countryvore = Boolean.parseBoolean(
                reader.configurationValue(configuration, CONFIGURATION_COUNTRYVORE, "false"));
    }

    public Function<Shard, Optional<Atlas>> getFetcher(final String atlasPath, final String country,
            final Map<String, String> sparkConfiguration)
    {
        final Map<String, Object> atlasProviderContext = new HashMap<>();
        atlasProviderContext.put(AtlasProvider.AtlasProviderConstants.FILE_PATH_KEY,
                atlasPath + this.inputDependencyPath);
        atlasProviderContext.put(AtlasProvider.AtlasProviderConstants.SPARK_CONFIGURATION_KEY,
                sparkConfiguration);
        this.atlasProvider.setAtlasProviderContext(atlasProviderContext);

        if (this.countryvore)
        {
            return getCountryvoreFetcher(atlasPath, country);
        }
        else
        {
            return getSingleCountryFetcher(atlasPath, country);
        }
    }

    public Optional<String> getInputDependencyName()
    {
        return "".equals(this.inputDependencyName) ? Optional.empty()
                : Optional.of(this.inputDependencyName);
    }

    public String getName()
    {
        return this.name;
    }

    public ConfiguredSubAtlas getSubAtlas()
    {
        return this.subAtlas;
    }

    public JsonObject toJson()
    {
        final JsonObject fetcherObject = new JsonObject();
        fetcherObject.addProperty(AtlasMutatorConfiguration.TYPE_JSON_PROPERTY,
                TYPE_JSON_PROPERTY_VALUE);
        fetcherObject.addProperty(NAME_JSON_PROPERTY, this.name);
        if (this.inputDependencyName != null && !this.inputDependencyName.isEmpty())
        {
            fetcherObject.addProperty(INPUT_DEPENDENCY_NAME_JSON_PROPERTY,
                    this.inputDependencyName); // NOSONAR
        }
        if (this.inputDependencyPath != null && !this.inputDependencyPath.isEmpty())
        {
            fetcherObject.addProperty(INPUT_DEPENDENCY_PATH_JSON_PROPERTY,
                    this.inputDependencyPath); // NOSONAR
        }
        if (this.subAtlas != null)
        {
            fetcherObject.addProperty(SUB_ATLAS_JSON_PROPERTY, this.subAtlas.getName()); // NOSONAR
        }
        return fetcherObject;
    }

    @Override
    public String toString()
    {
        return this.name;
    }

    public ConfiguredAtlasFetcher withShardsToCountries(
            final MultiMapWithSet<Shard, String> shardsToCountries)
    {
        if (this.countryvore)
        {
            this.shardsToCountries = shardsToCountries;
        }
        return this;
    }

    private Function<Shard, Optional<Atlas>> getCountryvoreFetcher(final String atlasPath,
            final String country)
    {
        if (!this.countryvore)
        {
            throw new CoreException(
                    "Should not request countryvore fetcher for a non-countryvore configured fetcher."
                            + " Request happened for country {} at {}.",
                    country, atlasPath);
        }
        if (this.shardsToCountries == null || this.shardsToCountries.isEmpty())
        {
            throw new CoreException(
                    "Countryvore fetcher for {} at {} needs to have a full shard to country list. "
                            + "This exception means it was not propagated properly.",
                    country, atlasPath);
        }
        return (Serializable & Function<Shard, Optional<Atlas>>) shardSource ->
        {
            if (!this.shardsToCountries.containsKey(shardSource))
            {
                return Optional.empty();
            }
            final Set<Atlas> preCountryvoreAtlases = this.shardsToCountries.get(shardSource)
                    .stream().map(subCountry ->
                    {
                        final Function<Shard, Optional<Atlas>> singleCountryFetcher = getSingleCountryFetcher(
                                atlasPath, subCountry);
                        return singleCountryFetcher.apply(shardSource);
                    }).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toSet());
            if (preCountryvoreAtlases.isEmpty())
            {
                return Optional.empty();
            }
            else if (preCountryvoreAtlases.size() == 1)
            {
                return Optional.ofNullable(preCountryvoreAtlases.iterator().next());
            }
            else
            {
                return Optional
                        .ofNullable(new MultiAtlas(preCountryvoreAtlases).cloneToPackedAtlas());
            }
        };
    }

    private Function<Shard, Optional<Atlas>> getSingleCountryFetcher(final String atlasPath,
            final String country)
    {
        return (Serializable & Function<Shard, Optional<Atlas>>) shardSource ->
        {
            final Optional<Atlas> atlasOption = this.atlasProvider.apply(country, shardSource);
            if (atlasOption.isPresent())
            {
                return this.subAtlas.apply(atlasOption.get());
            }
            return Optional.empty();
        };
    }
}
