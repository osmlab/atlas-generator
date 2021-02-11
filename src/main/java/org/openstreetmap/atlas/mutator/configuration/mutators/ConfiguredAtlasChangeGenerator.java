package org.openstreetmap.atlas.mutator.configuration.mutators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.exception.change.FeatureChangeMergeException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.geography.atlas.change.AtlasEntityKey;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.ChangeAtlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChangeBoundsExpander;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.InputDependency;
import org.openstreetmap.atlas.mutator.configuration.broadcast.ConfiguredBroadcastable;
import org.openstreetmap.atlas.mutator.configuration.broadcast.ConfiguredBroadcastableBuilder;
import org.openstreetmap.atlas.mutator.configuration.parsing.AtlasMutatorConfigurationParser;
import org.openstreetmap.atlas.mutator.configuration.parsing.ConfiguredDynamicAtlasPolicy;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.ConfiguredMergeForgivenessPolicy;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;
import org.openstreetmap.atlas.utilities.filters.AtlasEntityPolygonsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * A configurable {@link AtlasChangeGenerator}. It contains default configuration elements, like
 * dependencies, and enabled or not.
 *
 * @author matthieun
 */
public abstract class ConfiguredAtlasChangeGenerator implements AtlasChangeGenerator
{
    public static final String CONFIGURATION_DEPENDENCIES = "dependsOn";
    public static final String CONFIGURATION_COUNTRIES = "countries";
    public static final String CONFIGURATION_EXCLUDED_COUNTRIES = "excludedCountries";
    public static final String CONFIGURATION_VALIDATE = AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL
            + ".validate";
    public static final String CONFIGURATION_ENABLED = "enabled";
    public static final String CONFIGURATION_DYNAMIC_ATLAS_POLICY_GENERATION = "dynamicAtlas.generation";
    public static final String CONFIGURATION_DYNAMIC_ATLAS_POLICY_APPLICATION = "dynamicAtlas.application";
    public static final String CONFIGURATION_BROADCAST_VARIABLES_NEEDED = "broadcast";
    public static final String CONFIGURATION_CLASS_NAME = "className";

    public static final String TYPE_JSON_PROPERTY_VALUE = "_mutator";
    public static final String NAME_JSON_PROPERTY = "name";
    public static final String CLASS_NAME_JSON_PROPERTY = "className";
    public static final String DEPENDENCIES_JSON_PROPERTY = "dependencies";
    public static final String COUNTRIES_JSON_PROPERTY = "countries";
    public static final String EXCLUDED_COUNTRIES_JSON_PROPERTY = "excludedCountries";
    public static final String ENABLED_JSON_PROPERTY = "enabled";
    public static final String DYNAMIC_ATLAS_POLICY_GENERATION_JSON_PROPERTY = "dynamicAtlasPolicyGeneration";
    public static final String DYNAMIC_ATLAS_POLICY_APPLICATION_JSON_PROPERTY = "dynamicAtlasPolicyApplication";
    public static final String INPUT_DEPENDENCIES_JSON_PROPERTY = "inputDependencies";
    public static final String DECLARED_MERGE_FORGIVENESS_POLICY_JSON_PROPERTY = "declaredMergeForgivenessPolicy";
    public static final String BROADCAST_VARIABLES_NEEDED_JSON_PROPERTY = "broadcastVariablesNeeded";

    private static final Logger logger = LoggerFactory
            .getLogger(ConfiguredAtlasChangeGenerator.class);
    private static final long serialVersionUID = 2494966710390021964L;

    private final String name;
    private final Set<String> dependencies;
    private final Set<String> countries;
    private final Set<String> excludedCountries;
    private final boolean enabled;
    private final boolean validate;
    private final AtlasEntityPolygonsFilter polygonFilter;
    private final AtlasEntityPolygonsFilter globalPolygonFilter;
    private final ConfiguredDynamicAtlasPolicy dynamicAtlasPolicyGeneration;
    private final ConfiguredDynamicAtlasPolicy dynamicAtlasPolicyApplication;
    private final ConfiguredMergeForgivenessPolicy mergeForgivenessPolicy;
    private final List<InputDependency> inputDependencies;
    private final String className;
    private Map<String, String> sparkConfiguration = Maps.hashMap();
    private transient Map<String, Object> broadcastVariables;
    private final Map<String, ConfiguredBroadcastable> broadcastVariablesNeeded;

    public ConfiguredAtlasChangeGenerator(final String name, final Configuration configuration)
    {
        this.name = name;
        final ConfigurationReader reader = new ConfigurationReader(name);
        this.dependencies = new HashSet<>(reader.configurationValue(configuration,
                CONFIGURATION_DEPENDENCIES, new ArrayList<>()));
        this.countries = new HashSet<>(reader.configurationValue(configuration,
                CONFIGURATION_COUNTRIES, new ArrayList<>()));
        this.excludedCountries = new HashSet<>(reader.configurationValue(configuration,
                CONFIGURATION_EXCLUDED_COUNTRIES, new ArrayList<>()));
        final Set<String> countryIntersection = Sets.intersection(this.countries,
                this.excludedCountries);
        if (!countryIntersection.isEmpty())
        {
            throw new CoreException("{}: Included and excluded countries are overlapping for {}",
                    name, countryIntersection);
        }
        this.enabled = reader.configurationValue(configuration, CONFIGURATION_ENABLED, false);
        this.className = reader.configurationValue(configuration, CONFIGURATION_CLASS_NAME, "");
        // Validation is global, use a different reader for it.
        this.validate = new ConfigurationReader("").configurationValue(configuration,
                CONFIGURATION_VALIDATE, false);
        this.globalPolygonFilter = AtlasEntityPolygonsFilter.forConfiguration(configuration);
        this.polygonFilter = AtlasEntityPolygonsFilter.forConfigurationValues(
                reader.configurationValue(configuration,
                        AtlasEntityPolygonsFilter.INCLUDED_POLYGONS_KEY, Collections.emptyMap()),
                reader.configurationValue(configuration,
                        AtlasEntityPolygonsFilter.INCLUDED_MULTIPOLYGONS_KEY,
                        Collections.emptyMap()),
                reader.configurationValue(configuration,
                        AtlasEntityPolygonsFilter.EXCLUDED_POLYGONS_KEY, Collections.emptyMap()),
                reader.configurationValue(configuration,
                        AtlasEntityPolygonsFilter.EXCLUDED_MULTIPOLYGONS_KEY,
                        Collections.emptyMap()));
        this.dynamicAtlasPolicyGeneration = ConfiguredDynamicAtlasPolicy.from(reader
                .configurationValue(configuration, CONFIGURATION_DYNAMIC_ATLAS_POLICY_GENERATION,
                        ConfiguredDynamicAtlasPolicy.DEFAULT_NAME),
                configuration);
        this.dynamicAtlasPolicyApplication = ConfiguredDynamicAtlasPolicy.from(reader
                .configurationValue(configuration, CONFIGURATION_DYNAMIC_ATLAS_POLICY_APPLICATION,
                        ConfiguredDynamicAtlasPolicy.DEFAULT_NAME),
                configuration);
        this.mergeForgivenessPolicy = ConfiguredMergeForgivenessPolicy.fromRoot(configuration,
                this.name);
        logger.debug("For {} read mergeForgivenessPolicy\n{}", this.name,
                this.mergeForgivenessPolicy);
        final Set<String> inputDependencyNames = new HashSet<>();
        this.dynamicAtlasPolicyGeneration.getInputDependencyName()
                .ifPresent(inputDependencyNames::add);
        this.dynamicAtlasPolicyApplication.getInputDependencyName()
                .ifPresent(inputDependencyNames::add);
        this.inputDependencies = inputDependencyNames.stream()
                .map(inputDependencyName -> new InputDependency(this, inputDependencyName,
                        configuration))
                .collect(Collectors.toList());
        final Set<String> broadcastVariableNames = new HashSet<>(reader.configurationValue(
                configuration, CONFIGURATION_BROADCAST_VARIABLES_NEEDED, new ArrayList<>()));
        final ConfiguredBroadcastableBuilder configuredBroadcastableBuilder = new ConfiguredBroadcastableBuilder(
                configuration);
        this.broadcastVariablesNeeded = new HashMap<>();
        broadcastVariableNames.forEach(
                broadcastVariableName -> this.broadcastVariablesNeeded.put(broadcastVariableName,
                        configuredBroadcastableBuilder.create(broadcastVariableName)));
    }

    public void addBroadcastVariable(final String name, final Object broadcastVariable)
    {
        if (this.broadcastVariables == null)
        {
            this.broadcastVariables = new HashMap<>();
        }
        this.broadcastVariables.put(name, broadcastVariable);
    }

    @Override
    public boolean equals(final Object other)
    {
        if (other instanceof ConfiguredAtlasChangeGenerator)
        {
            return this.getName().equals(((ConfiguredAtlasChangeGenerator) other).getName())
                    && this.dynamicAtlasPolicyGeneration
                            .equals(((ConfiguredAtlasChangeGenerator) other)
                                    .getDynamicAtlasPolicyGeneration())
                    && this.dynamicAtlasPolicyApplication
                            .equals(((ConfiguredAtlasChangeGenerator) other)
                                    .getDynamicAtlasPolicyApplication());
        }
        return false;
    }

    @Override
    public Set<FeatureChange> generate(final Atlas atlas)
    {
        final Map<AtlasEntityKey, Integer> identifierToIndex = new HashMap<>();
        final List<FeatureChange> mergedResult = new ArrayList<>();

        // TODO figure out a way to not have to call expandNodeBounds here?
        final Set<FeatureChange> rawResult = new FeatureChangeBoundsExpander(
                generateWithoutValidation(atlas), atlas).apply();
        rawResult.stream().forEach(featureChange -> featureChange.withAtlasContext(atlas));

        if (rawResult.isEmpty())
        {
            return rawResult;
        }

        for (final FeatureChange rawFeatureChange : rawResult)
        {
            final int currentIndex = mergedResult.size();
            final AtlasEntityKey key = AtlasEntityKey.from(rawFeatureChange.getItemType(),
                    rawFeatureChange.getIdentifier());

            FeatureChange resultingFeatureChange = rawFeatureChange;
            if (!identifierToIndex.containsKey(key))
            {
                identifierToIndex.put(key, currentIndex);
                mergedResult.add(resultingFeatureChange);
            }
            else
            {
                final int existingIndex = identifierToIndex.get(key);
                final FeatureChange existing = mergedResult.get(existingIndex);
                try
                {
                    resultingFeatureChange = existing.merge(rawFeatureChange);
                }
                catch (final FeatureChangeMergeException exception)
                {
                    final Optional<FeatureChange> featureChangeFromPolicy = this.mergeForgivenessPolicy
                            .applyPolicy(exception, existing, rawFeatureChange);
                    if (featureChangeFromPolicy.isEmpty())
                    {
                        throw new CoreException(
                                "Conflict merging internal {}, failed to apply forgiveness policy:\n{}\nFeatureChanges:\n{}\nvs\n{}",
                                this.name, this.mergeForgivenessPolicy, existing.prettify(),
                                rawFeatureChange.prettify(), exception);
                    }
                    logger.warn(
                            "Conflict merging internal {}, successfully applied forgiveness policy:\n{}\nFeatureChanges:\n{}\nvs\n{}\nChose:\n{}",
                            this.name, this.mergeForgivenessPolicy, existing.prettify(),
                            rawFeatureChange.prettify(), featureChangeFromPolicy.get().prettify(),
                            exception);
                    resultingFeatureChange = featureChangeFromPolicy.get();
                }
                mergedResult.set(existingIndex, resultingFeatureChange);
            }
        }

        return new HashSet<>(mergedResult);
    }

    public Map<String, ConfiguredBroadcastable> getBroadcastVariablesNeeded()
    {
        return this.broadcastVariablesNeeded;
    }

    /**
     * @return The countries this generator applies to.
     */
    public Set<String> getCountries()
    {
        return this.countries;
    }

    public Set<String> getDependencies()
    {
        return this.dependencies;
    }

    public ConfiguredDynamicAtlasPolicy getDynamicAtlasPolicyApplication()
    {
        return this.dynamicAtlasPolicyApplication;
    }

    public ConfiguredDynamicAtlasPolicy getDynamicAtlasPolicyGeneration()
    {
        return this.dynamicAtlasPolicyGeneration;
    }

    public Set<String> getExcludedCountries()
    {
        return this.excludedCountries;
    }

    public AtlasEntityPolygonsFilter getGlobalPolygonFilter()
    {
        return this.globalPolygonFilter;
    }

    public List<InputDependency> getInputDependencies()
    {
        return this.inputDependencies;
    }

    public ConfiguredMergeForgivenessPolicy getMergeForgivenessPolicy()
    {
        return this.mergeForgivenessPolicy;
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    public AtlasEntityPolygonsFilter getPolygonFilter()
    {
        return this.polygonFilter;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(getName());
    }

    public boolean isEnabled()
    {
        return this.enabled;
    }

    public JsonObject toJson()
    {
        final JsonObject mutatorObject = new JsonObject();
        final JsonArray dependenciesArray = new JsonArray();
        final JsonArray inputDependenciesArray = new JsonArray();
        final JsonArray broadcastVariablesNeededArray = new JsonArray();

        mutatorObject.addProperty(AtlasMutatorConfiguration.TYPE_JSON_PROPERTY,
                TYPE_JSON_PROPERTY_VALUE);
        mutatorObject.addProperty(NAME_JSON_PROPERTY, this.name);
        if (!this.className.isEmpty())
        {
            mutatorObject.addProperty(CLASS_NAME_JSON_PROPERTY, this.className);
        }
        mutatorObject.addProperty(ENABLED_JSON_PROPERTY, this.enabled);
        mutatorObject.addProperty(DYNAMIC_ATLAS_POLICY_GENERATION_JSON_PROPERTY,
                this.dynamicAtlasPolicyGeneration.getName());
        mutatorObject.addProperty(DYNAMIC_ATLAS_POLICY_APPLICATION_JSON_PROPERTY,
                this.dynamicAtlasPolicyApplication.getName());

        if (!this.countries.isEmpty())
        {
            mutatorObject.addProperty(COUNTRIES_JSON_PROPERTY,
                    new StringList(this.countries).join(","));
        }

        if (!this.excludedCountries.isEmpty())
        {
            mutatorObject.addProperty(EXCLUDED_COUNTRIES_JSON_PROPERTY,
                    new StringList(this.excludedCountries).join(","));
        }

        for (final String dependency : this.dependencies)
        {
            dependenciesArray.add(new JsonPrimitive(dependency));
        }
        if (dependenciesArray.size() > 0)
        {
            mutatorObject.add(DEPENDENCIES_JSON_PROPERTY, dependenciesArray);
        }

        for (final InputDependency inputDependency : this.inputDependencies)
        {
            inputDependenciesArray.add(new JsonPrimitive(inputDependency.getPathName()));
        }
        if (inputDependenciesArray.size() > 0)
        {
            mutatorObject.add(INPUT_DEPENDENCIES_JSON_PROPERTY, inputDependenciesArray);
        }

        mutatorObject.addProperty(DECLARED_MERGE_FORGIVENESS_POLICY_JSON_PROPERTY,
                !this.mergeForgivenessPolicy.policyIsEmpty());

        for (final Map.Entry<String, ConfiguredBroadcastable> entry : this.broadcastVariablesNeeded
                .entrySet())
        {
            broadcastVariablesNeededArray.add(new JsonPrimitive(entry.getValue().getName()));
        }
        if (broadcastVariablesNeededArray.size() > 0)
        {
            mutatorObject.add(BROADCAST_VARIABLES_NEEDED_JSON_PROPERTY,
                    broadcastVariablesNeededArray);
        }

        return mutatorObject;
    }

    @Override
    public String toString()
    {
        return getName();
    }

    @Override
    public void validate(final Atlas source, final Change change)
    {
        if (this.validate)
        {
            new ChangeAtlas(source, change).validate();
        }
    }

    protected Object getBroadcastVariable(final String name)
    {
        final Object result = this.broadcastVariables.get(name);
        if (result == null)
        {
            throw new CoreException("Broadcast variable not found: \"{}\"", name);
        }
        return result;
    }

    protected Map<String, String> getSparkConfiguration()
    {
        return new HashMap<>(this.sparkConfiguration);
    }

    void setSparkConfiguration(final Map<String, String> sparkConfiguration)
    {
        this.sparkConfiguration = new HashMap<>(sparkConfiguration);
    }
}
