package org.openstreetmap.atlas.mutator.configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.converters.jts.JtsPolygonToMultiPolygonConverter;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.mutator.AtlasMutator;
import org.openstreetmap.atlas.mutator.configuration.parsing.AtlasMutatorConfigurationParser;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.ConfiguredMergeForgivenessPolicy;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.maps.MultiMap;
import org.openstreetmap.atlas.utilities.tuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Class that holds global configuration for an {@link AtlasMutator}
 *
 * @author matthieun
 */
public class AtlasMutatorConfiguration implements Serializable
{
    public static final String GROUP_PREFIX = "GROUP";

    public static final String TYPE_JSON_PROPERTY = "type";
    public static final String MUTATORS_JSON_PROPERTY = "mutators";
    public static final String LEVELS_JSON_PROPERTY = "levels";
    public static final String DYNAMIC_ATLAS_POLICIES_JSON_PROPERTY = "dynamicAtlasPolicies";
    public static final String FILTERS_JSON_PROPERTY = "filters";
    public static final String INPUT_DEPENDENCIES_JSON_PROPERTY = "inputDependencies";
    public static final String FETCHERS_JSON_PROPERTY = "fetchers";
    public static final String SUB_ATLASES_JSON_PROPERTY = "subAtlases";
    public static final String MERGE_FORGIVENESS_POLICIES_JSON_PROPERTY = "mergeForgivenessPolicies";
    public static final String BROADCASTABLES_JSON_PROPERTY = "broadcastables";

    private static final long serialVersionUID = 6108679410416988121L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasMutatorConfiguration.class);

    private final Sharding sharding;
    private final transient CountryBoundaryMap boundaries;
    private final String input;
    private final String output;
    private final Map<String, String> sparkConfiguration;
    private final Map<String, List<AtlasMutationLevel>> countryToMutationLevels;
    private final ConfiguredMergeForgivenessPolicy globalMergeForgivenessPolicy;

    public AtlasMutatorConfiguration(final Set<String> countries, final Sharding sharding, // NOSONAR
            final CountryBoundaryMap boundaries, final String input, final String output,
            final Map<String, String> sparkConfiguration, final Configuration mutatorsConfiguration,
            final boolean groupCountries, final boolean allowRDD, final boolean preloadRDD,
            final boolean validateBoundaries)
    {
        this.sharding = sharding;
        this.boundaries = boundaries;
        this.input = input;
        this.output = output;
        this.sparkConfiguration = sparkConfiguration;
        this.countryToMutationLevels = new AtlasMutatorConfigurationParser(this,
                mutatorsConfiguration, sparkConfiguration).generateCountryToMutationLevels(
                        countries, groupCountries, allowRDD, preloadRDD);
        this.globalMergeForgivenessPolicy = ConfiguredMergeForgivenessPolicy
                .fromGlobal(mutatorsConfiguration);
        logger.debug("For global read mergeForgivenessPolicy\n{}",
                this.globalMergeForgivenessPolicy);
        if (validateBoundaries)
        {
            this.validateCountryAndBoundaries(countries);
        }
    }

    public AtlasMutatorConfiguration(final Set<String> countries, final Sharding sharding, // NOSONAR
            final CountryBoundaryMap boundaries, final String input, final String output,
            final Map<String, String> sparkConfiguration, final Configuration mutatorsConfiguration,
            final boolean groupCountries, final boolean allowRDD, final boolean preloadRDD)
    {
        this(countries, sharding, boundaries, input, output, sparkConfiguration,
                mutatorsConfiguration, groupCountries, allowRDD, preloadRDD, true);
    }

    public MultiPolygon boundary(final String country)
    {
        if (this.boundaries.countryBoundary(country) == null)
        {
            throw new CoreException("Country {} has no boundaries in the boundary file", country);
        }
        final JtsPolygonToMultiPolygonConverter jtsPolygonToMultiPolygonConverter = new JtsPolygonToMultiPolygonConverter();
        return this.boundaries.countryBoundary(country).stream()
                .map(jtsPolygonToMultiPolygonConverter::convert)
                .reduce(new MultiPolygon(new MultiMap<>()), (left, right) ->
                {
                    final MultiMap<Polygon, Polygon> outersToInners = new MultiMap<>();
                    outersToInners.addAll(left.getOuterToInners());
                    outersToInners.addAll(right.getOuterToInners());
                    return new MultiPolygon(outersToInners);
                });
    }

    /**
     * Get a detailed country or group summary from this {@link AtlasMutatorConfiguration} as a
     * {@link Tuple}. The first element is another {@link Tuple} containing the group info and the
     * constituent countries (e.g. {GROUP0, [DMA, USA]} or {DMA, [DMA]}). The second element is a
     * {@link List} containing all the levels for the given country or group.
     *
     * @param countryOrGroup
     *            the country or group for which to fetch the levels
     * @return the {@link Tuple} containing the levels, {@code null} if the countryOrGroup is not
     *         found
     */
    public Tuple<Tuple<String, Set<String>>, List<AtlasMutationLevel>> countryOrGroupAndLevelList(
            final String countryOrGroup)
    {
        final List<AtlasMutationLevel> levels = this.countryToMutationLevels.get(countryOrGroup);
        if (levels != null)
        {
            final Set<String> countries = levels.iterator().next().getCountries();
            return new Tuple<>(new Tuple<>(countryOrGroup, countries), levels);
        }
        return null;
    }

    /**
     * Get a detailed summary of this {@link AtlasMutatorConfiguration} as a {@link Map}. The
     * {@link Map} keys are {@link Tuple}s containing the group info and the constituent countries
     * (e.g. {GROUP0, [DMA, USA]} or {DMA, [DMA]}). The {@link Map} values are {@link List}s
     * containing the all levels for the country or group key.
     *
     * @return the {@link Map} containing the levels
     */
    public Map<Tuple<String, Set<String>>, List<AtlasMutationLevel>> countryOrGroupsToLevelLists()
    {
        final Map<Tuple<String, Set<String>>, List<AtlasMutationLevel>> result = new HashMap<>();
        for (final String countryOrGroupKey : this.countryToMutationLevels.keySet())
        {
            final Tuple<Tuple<String, Set<String>>, List<AtlasMutationLevel>> countryOrGroupTolevel = this
                    .countryOrGroupAndLevelList(countryOrGroupKey);
            if (countryOrGroupTolevel == null)
            {
                throw new CoreException(
                        "Key {} not found in countryToMutationLevels, this should not happen!",
                        countryOrGroupKey);
            }
            result.put(countryOrGroupTolevel.getFirst(), countryOrGroupTolevel.getSecond());
        }
        return result;
    }

    /**
     * Get a detailed summary of this {@link AtlasMutatorConfiguration} as a {@link Map}. The
     * {@link Map} keys are {@link Tuple}s containing the group info and the constituent countries
     * (e.g. {GROUP0, [DMA, USA]} or {DMA, [DMA]}). The {@link Map} values are {@link String}s
     * containing the level details (e.g. GROUP0 level 0 might look like "GROUP0_0:
     * FS,AtlasChangeGeneratorAddTurnRestrictions").
     *
     * @return the {@link Map} containing the details
     */
    public Map<Tuple<String, Set<String>>, String> details()
    {
        final Map<Tuple<String, Set<String>>, String> result = new HashMap<>();
        for (final String countryOrGroupKey : this.countryToMutationLevels.keySet())
        {
            final Tuple<Tuple<String, Set<String>>, String> details = this
                    .details(countryOrGroupKey);
            if (details == null)
            {
                throw new CoreException(
                        "Key {} not found in countryToMutationLevels, this should not happen!",
                        countryOrGroupKey);
            }
            result.put(details.getFirst(), details.getSecond());
        }
        return result;
    }

    /**
     * Get a detailed country or group summary from this {@link AtlasMutatorConfiguration} as a
     * {@link Tuple}. The first element is another {@link Tuple} containing the group info and the
     * constituent countries (e.g. {GROUP0, [DMA, USA]} or {DMA, [DMA]}). The second element is a
     * {@link String} containing the level details (e.g. GROUP0 level 0 might look like "GROUP0_0:
     * FS,AtlasChangeGeneratorAddTurnRestrictions").
     *
     * @param countryOrGroup
     *            the country or group for which to fetch the details
     * @return the {@link Tuple} containing the details, {@code null} if the countryOrGroup is not
     *         found
     */
    public Tuple<Tuple<String, Set<String>>, String> details(final String countryOrGroup)
    {
        final List<AtlasMutationLevel> levels = this.countryToMutationLevels.get(countryOrGroup);
        if (levels != null)
        {
            final Set<String> countries = levels.iterator().next().getCountries();
            final StringList levelsStringList = new StringList();
            for (final AtlasMutationLevel level : levels)
            {
                levelsStringList.add("    " + level.details());
            }
            return new Tuple<>(new Tuple<>(countryOrGroup, countries),
                    levelsStringList.join(System.lineSeparator()));
        }
        return null;
    }

    /**
     * Get a string-ified version of the details {@link Tuple} returned by
     * {@link AtlasMutatorConfiguration#details(String)}.
     *
     * @param countryOrGroup
     *            the country or group for which to fetch the details string
     * @return the details for a given country or group in this configuration, as a string.
     *         {@code null} if the countryOrGroup is not found
     */
    public String detailsString(final String countryOrGroup)
    {
        final Tuple<Tuple<String, Set<String>>, String> detailsTuple = details(countryOrGroup);
        final StringList countryAndLevels = new StringList();
        if (detailsTuple != null)
        {
            final Tuple<String, Set<String>> countryTuple = detailsTuple.getFirst();
            final String levels = detailsTuple.getSecond();
            String logMessage = "AtlasMutator %s and levels for ";
            if (countryTuple.getFirst().startsWith(AtlasMutatorConfiguration.GROUP_PREFIX))
            {
                logMessage = String.format(logMessage, "group");
            }
            else
            {
                logMessage = String.format(logMessage, "country");
            }
            final StringList countryList = new StringList(countryTuple.getSecond());
            final String countryListDescription = " (" + countryList.join(", ") + ")";
            countryAndLevels.add(logMessage + countryTuple.getFirst()
                    + (countryList.size() > 1 ? countryListDescription : "") + ":");
            countryAndLevels.add(levels);
            return countryAndLevels.join(System.lineSeparator());
        }
        return null;
    }

    /**
     * Get a string-ified version of the details {@link Map} returned by
     * {@link AtlasMutatorConfiguration#details()}. The details will be lexicographically sorted by
     * the country or group name.
     *
     * @return the details of this configuration, as a string
     */
    public String detailsString()
    {
        final StringList countryAndLevels = new StringList();
        for (final String countryOrGroupKey : new TreeSet<>(this.countryToMutationLevels.keySet()))
        {
            final String detailsForCountry = detailsString(countryOrGroupKey);
            if (detailsForCountry == null)
            {
                throw new CoreException(
                        "Key {} not found in countryToMutationLevels, this should not happen!",
                        countryOrGroupKey);
            }
            countryAndLevels.add(detailsForCountry);
        }
        return countryAndLevels.join(System.lineSeparator());
    }

    public CountryBoundaryMap getBoundaries()
    {
        return this.boundaries;
    }

    public AtlasMutatorConfigurationDetails getConfigurationDetails()
    {
        return new AtlasMutatorConfigurationDetails(this);
    }

    /**
     * @return A map from country code to ordered list of mutation levels.
     */
    public Map<String, List<AtlasMutationLevel>> getCountryToMutationLevels()
    {
        return this.countryToMutationLevels;
    }

    public ConfiguredMergeForgivenessPolicy getGlobalMergeForgivenessPolicy()
    {
        return this.globalMergeForgivenessPolicy;
    }

    public String getInput()
    {
        return this.input;
    }

    public String getOutput()
    {
        return this.output;
    }

    public Sharding getSharding()
    {
        return this.sharding;
    }

    public Map<String, String> getSparkConfiguration()
    {
        return this.sparkConfiguration;
    }

    /**
     * Check if this {@link AtlasMutatorConfiguration} is empty. An empty configuration is one that
     * contains an empty country to levels map.
     *
     * @return if this {@link AtlasMutatorConfiguration} is empty.
     */
    public boolean isEmpty()
    {
        return this.countryToMutationLevels.isEmpty();
    }

    public JsonObject toJson()
    {
        final JsonObject configurationJson = new JsonObject();
        final JsonArray mutatorObjects = new JsonArray();
        final JsonArray levelObjects = new JsonArray();
        final JsonArray dynamicAtlasPolicyObjects = new JsonArray();
        final JsonArray filterObjects = new JsonArray();
        final JsonArray inputDependencyObjects = new JsonArray();
        final JsonArray fetcherObjects = new JsonArray();
        final JsonArray subAtlasObjects = new JsonArray();
        final JsonArray mergeForgivenessPolicyObjects = new JsonArray();
        final JsonArray broadcastableObjects = new JsonArray();

        final AtlasMutatorConfigurationDetails configurationDetails = this
                .getConfigurationDetails();

        for (final Map.Entry<String, List<AtlasMutationLevel>> countryOrGroupAndLevels : this.countryToMutationLevels
                .entrySet())
        {
            countryOrGroupAndLevels.getValue().stream().map(AtlasMutationLevel::toJson)
                    .forEach(levelObjects::add);
        }

        for (final String mutatorName : new TreeSet<>(configurationDetails.getMutators().keySet()))
        {
            mutatorObjects.add(configurationDetails.getMutators().get(mutatorName).toJson());
        }

        for (final String policyName : new TreeSet<>(
                configurationDetails.getDynamicAtlasPolicies().keySet()))
        {
            dynamicAtlasPolicyObjects
                    .add(configurationDetails.getDynamicAtlasPolicies().get(policyName).toJson());
        }

        for (final String filterName : new TreeSet<>(configurationDetails.getFilters().keySet()))
        {
            filterObjects.add(configurationDetails.getFilters().get(filterName).toJson());
        }

        for (final String dependencyName : new TreeSet<>(
                configurationDetails.getInputDependencies().keySet()))
        {
            inputDependencyObjects
                    .add(configurationDetails.getInputDependencies().get(dependencyName).toJson());
        }

        for (final String fetcherName : new TreeSet<>(configurationDetails.getFetchers().keySet()))
        {
            fetcherObjects.add(configurationDetails.getFetchers().get(fetcherName).toJson());
        }

        for (final String subAtlasName : new TreeSet<>(
                configurationDetails.getSubAtlases().keySet()))
        {
            subAtlasObjects.add(configurationDetails.getSubAtlases().get(subAtlasName).toJson());
        }

        for (final String mergeForgivenessPolicyName : new TreeSet<>(
                configurationDetails.getMergeForgivenessPolicies().keySet()))
        {
            mergeForgivenessPolicyObjects.add(configurationDetails.getMergeForgivenessPolicies()
                    .get(mergeForgivenessPolicyName).toJson());
        }

        for (final String broadcastableName : new TreeSet<>(
                configurationDetails.getBroadcastables().keySet()))
        {
            broadcastableObjects
                    .add(configurationDetails.getBroadcastables().get(broadcastableName).toJson());
        }

        configurationJson.add(MUTATORS_JSON_PROPERTY, mutatorObjects);
        configurationJson.add(LEVELS_JSON_PROPERTY, levelObjects);
        configurationJson.add(DYNAMIC_ATLAS_POLICIES_JSON_PROPERTY, dynamicAtlasPolicyObjects);
        configurationJson.add(FILTERS_JSON_PROPERTY, filterObjects);
        configurationJson.add(INPUT_DEPENDENCIES_JSON_PROPERTY, inputDependencyObjects);
        configurationJson.add(FETCHERS_JSON_PROPERTY, fetcherObjects);
        configurationJson.add(SUB_ATLASES_JSON_PROPERTY, subAtlasObjects);
        configurationJson.add(MERGE_FORGIVENESS_POLICIES_JSON_PROPERTY,
                mergeForgivenessPolicyObjects);
        configurationJson.add(BROADCASTABLES_JSON_PROPERTY, broadcastableObjects);
        return configurationJson;
    }

    @Override
    public String toString()
    {
        return "AtlasMutatorConfiguration [input=" + this.input + ", output=" + this.output + "]";
    }

    private void validateCountryAndBoundaries(final Set<String> countries)
    {
        if (this.boundaries == null)
        {
            // No boundaries, unable to check
            return;
        }
        final var missingCountries = countries.stream().filter(
                country -> !this.boundaries.getCountryNameToBoundaryMap().containsKey(country))
                .collect(Collectors.toSet());
        if (!missingCountries.isEmpty())
        {
            throw new CoreException("Country(ies) {} do(es) not have a boundary.",
                    missingCountries);
        }
    }
}
