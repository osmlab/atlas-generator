package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGeneratorBuilder;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.graphs.DirectedAcyclicGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read all the mutations in the configuration files, and index them by name and country.
 * 
 * @author matthieun
 */
public class AtlasMutatorIndex implements Serializable
{
    /**
     * @author matthieun
     */
    public static class Key implements Serializable
    {
        private static final long serialVersionUID = 2519299928829644206L;

        private final String country;
        private final String name;

        public Key(final String country, final String name)
        {
            this.country = country;
            this.name = name;
        }

        @Override
        public boolean equals(final Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (other == null || getClass() != other.getClass())
            {
                return false;
            }
            final Key key = (Key) other;
            return getCountry().equals(key.getCountry()) && getName().equals(key.getName());
        }

        public String getCountry()
        {
            return this.country;
        }

        public String getName()
        {
            return this.name;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getCountry(), getName());
        }
    }

    public static final String OVERRIDE_KEY = "override";

    private static final Logger logger = LoggerFactory.getLogger(AtlasMutatorIndex.class);
    private static final long serialVersionUID = -5540910714691494013L;

    private final Map<Key, ConfiguredAtlasChangeGenerator> index = new HashMap<>();
    private final transient ConfiguredAtlasChangeGeneratorBuilder configuredAtlasChangeGeneratorBuilder;

    public AtlasMutatorIndex(
            final ConfiguredAtlasChangeGeneratorBuilder configuredAtlasChangeGeneratorBuilder)
    {
        this.configuredAtlasChangeGeneratorBuilder = configuredAtlasChangeGeneratorBuilder;
    }

    protected DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator> getActionableDag(
            final String country)
    {
        final Map<String, ConfiguredAtlasChangeGenerator> filteredNameToGeneratorMap = filteredNameToGeneratorMap(
                country);
        final DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator> changeGeneratorDag = new DirectedAcyclicGraph<>();
        for (final Map.Entry<String, ConfiguredAtlasChangeGenerator> nameToGeneratorEntry : filteredNameToGeneratorMap
                .entrySet())
        {
            final ConfiguredAtlasChangeGenerator candidateGenerator = nameToGeneratorEntry
                    .getValue();
            final Set<ConfiguredAtlasChangeGenerator> dependsOn = dependsOn(country,
                    candidateGenerator, filteredNameToGeneratorMap);

            // Add to the DAG
            boolean generatorExists = false;
            for (final AtlasChangeGenerator generator : changeGeneratorDag
                    .getTopologicalSortedList())
            {
                if (generator.getName().equals(candidateGenerator.getName()))
                {
                    generatorExists = true;
                }
            }

            if (!generatorExists)
            {
                // Create the new node
                changeGeneratorDag.addVertex(candidateGenerator);
            }

            // Add all dependencies
            for (final ConfiguredAtlasChangeGenerator dependency : dependsOn)
            {
                logger.info("{}: Added dependency: {} to {}", country, dependency.getName(),
                        candidateGenerator.getName());
                final Optional<ConfiguredAtlasChangeGenerator> existingDependency = getGeneratorFromDag(
                        dependency, changeGeneratorDag);
                if (existingDependency.isPresent())
                {
                    changeGeneratorDag.addEdge(existingDependency.get(), candidateGenerator);
                }
                else
                {
                    changeGeneratorDag.addEdge(dependency, candidateGenerator);
                }
            }
        }
        return changeGeneratorDag;
    }

    protected void populate(final Set<String> countries, final Set<String> mutatorNames,
            final Configuration configuration)
    {
        for (final String country : countries)
        {
            final Configuration updatedConfiguration = updatedConfiguration(configuration, country);
            for (final String mutatorName : mutatorNames)
            {
                this.configuredAtlasChangeGeneratorBuilder
                        .create(country, mutatorName, updatedConfiguration).ifPresent(
                                mutator -> this.index.put(new Key(country, mutatorName), mutator));
            }
        }
    }

    protected Configuration updatedConfiguration(final Configuration source, final String country)
    {
        final Map<String, Object> configurationMap = new HashMap<>();
        for (final String key : source.configurationDataKeySet())
        {
            configurationMap.put(key, source.get(key).value());
        }
        // Update the configuration to have the right country overrides like
        // "override.XYZ.a.b.c" replace legitimate "a.b.c" configurations
        return new StandardConfiguration(source.toString(),
                updatedInternal(configurationMap, country));
    }

    /**
     * Get all the dependencies of a specific {@link AtlasChangeGenerator}
     *
     * @param country
     *            The country of interest
     * @param candidateGenerator
     *            The {@link AtlasChangeGenerator} of interest
     * @param allGenerators
     *            A map of name to {@link AtlasChangeGenerator} containing all the possible
     *            dependents, and then some.
     * @return all the dependencies of the provided {@link AtlasChangeGenerator}
     */
    private Set<ConfiguredAtlasChangeGenerator> dependsOn(final String country,
            final ConfiguredAtlasChangeGenerator candidateGenerator,
            final Map<String, ConfiguredAtlasChangeGenerator> allGenerators)
    {
        final Set<ConfiguredAtlasChangeGenerator> dependsOn = new HashSet<>();
        final String name = candidateGenerator.getName();
        for (final String dependency : candidateGenerator.getDependencies())
        {
            final Optional<ConfiguredAtlasChangeGenerator> dependentGenerator = Optional
                    .ofNullable(allGenerators.get(dependency));
            if (dependentGenerator.isPresent())
            {
                dependsOn.add(dependentGenerator.get());
            }
            else
            {
                logger.warn(
                        "{}: {} depends on {} which is not configured. The dependency will be ignored.",
                        country, name, dependency);
            }
        }
        return dependsOn;
    }

    private Map<String, ConfiguredAtlasChangeGenerator> filteredNameToGeneratorMap(
            final String country)
    {
        final Map<String, ConfiguredAtlasChangeGenerator> result = new HashMap<>();
        this.index.entrySet().stream().filter(entry -> country.equals(entry.getKey().getCountry()))
                .forEach(entry -> result.put(entry.getKey().getName(), entry.getValue()));
        return result;
    }

    private void findOverride(final Map.Entry<String, Object> entry,
            final Map<String, Object> overrides, final String country)
    {
        if (OVERRIDE_KEY.equals(entry.getKey()))
        {
            final Object value = entry.getValue();
            if (value instanceof HashMap)
            {
                final Map<String, Object> countryCodes = (HashMap) value;
                for (final Map.Entry<String, Object> countryCode : countryCodes.entrySet())
                {
                    if (country.equals(countryCode.getKey()))
                    {
                        final Object countryValue = countryCode.getValue();
                        if (countryValue instanceof HashMap)
                        {
                            final Map<String, Object> overridableValues = (HashMap) countryValue;
                            overrides.putAll(overridableValues);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param target
     *            The {@link AtlasChangeGenerator} looked after
     * @param dag
     *            The {@link DirectedAcyclicGraph} that potentially contains the target
     * @return An optional of the {@link AtlasChangeGenerator} that comes from the provided
     *         {@link DirectedAcyclicGraph} and matches the target.
     */
    private Optional<ConfiguredAtlasChangeGenerator> getGeneratorFromDag(
            final ConfiguredAtlasChangeGenerator target,
            final DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator> dag)
    {
        for (final ConfiguredAtlasChangeGenerator generator : dag.getTopologicalSortedList())
        {
            if (generator.getName().equals(target.getName()))
            {
                return Optional.of(generator);
            }
        }
        return Optional.empty();
    }

    /**
     * Recursive function that takes country-specific overrides like "override.XYZ.a.b.c" = "B"
     * would replace legitimate "a.b.c" = "A" configurations with "a.b.c" = "B" only for the country
     * XYZ
     *
     * @param source
     *            The original configuration
     * @param country
     *            The country of interest
     * @return The altered configuration
     */
    private Map<String, Object> updatedInternal(final Map<String, Object> source,
            final String country)
    {
        final Map<String, Object> configurationMap = new HashMap<>();
        final Map<String, Object> overrides = new HashMap<>();
        // 1st Pass. Find all the overrides and index them
        for (final Map.Entry<String, Object> entry : source.entrySet())
        {
            findOverride(entry, overrides, country);
        }
        // 2nd Pass. Find all the overridable entries and replace them
        for (final Map.Entry<String, Object> entry : source.entrySet())
        {
            final String key = entry.getKey();
            if (overrides.containsKey(key))
            {
                configurationMap.put(key, overrides.get(key));
            }
            else
            {
                final Object value = entry.getValue();
                if (value instanceof HashMap)
                {
                    final Map<String, Object> subValue = (HashMap) value;
                    configurationMap.put(key, updatedInternal(subValue, country));
                }
                else
                {
                    configurationMap.put(key, value);
                }
            }
        }
        return configurationMap;
    }
}
