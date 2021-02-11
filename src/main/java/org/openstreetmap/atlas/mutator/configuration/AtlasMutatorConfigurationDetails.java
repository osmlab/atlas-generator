package org.openstreetmap.atlas.mutator.configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openstreetmap.atlas.mutator.configuration.broadcast.ConfiguredBroadcastable;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.parsing.ConfiguredAtlasFetcher;
import org.openstreetmap.atlas.mutator.configuration.parsing.ConfiguredDynamicAtlasPolicy;
import org.openstreetmap.atlas.mutator.configuration.parsing.ConfiguredSubAtlas;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.ConfiguredMergeForgivenessPolicy;
import org.openstreetmap.atlas.utilities.configuration.ConfiguredFilter;

/**
 * Class that can compute details for an {@link AtlasMutatorConfiguration}.
 * 
 * @author lcram
 */
public class AtlasMutatorConfigurationDetails
{
    private final AtlasMutatorConfiguration configuration;
    private final Map<String, ConfiguredAtlasChangeGenerator> mutators;
    private final Map<String, ConfiguredDynamicAtlasPolicy> dynamicAtlasPolicies;
    private final Map<String, ConfiguredFilter> filters;
    private final Map<String, InputDependency> inputDependencies;
    private final Map<String, ConfiguredAtlasFetcher> fetchers;
    private final Map<String, ConfiguredSubAtlas> subAtlases;
    private final Map<String, ConfiguredMergeForgivenessPolicy> mergeForgivenessPolicies;
    private final Map<String, ConfiguredBroadcastable> broadcastables;

    public AtlasMutatorConfigurationDetails(final AtlasMutatorConfiguration configuration)
    {
        this.configuration = configuration;
        this.mutators = new HashMap<>();
        this.dynamicAtlasPolicies = new HashMap<>();
        this.filters = new HashMap<>();
        this.inputDependencies = new HashMap<>();
        this.fetchers = new HashMap<>();
        this.subAtlases = new HashMap<>();
        this.mergeForgivenessPolicies = new HashMap<>();
        this.broadcastables = new HashMap<>();
        discoverExtendedConfigurationMaps();
    }

    public Set<String> getAllConfigurationMapKeys()
    {
        final Set<String> allKeys = new HashSet<>();

        /*
         * Add all the TYPE JSON property values. These are things like "_mutator", "_level", etc.
         * This makes it easier to filter a search by type.
         */
        allKeys.addAll(Arrays.asList(ConfiguredAtlasChangeGenerator.TYPE_JSON_PROPERTY_VALUE,
                AtlasMutationLevel.TYPE_JSON_PROPERTY_VALUE,
                ConfiguredDynamicAtlasPolicy.TYPE_JSON_PROPERTY_VALUE,
                ConfiguredFilter.TYPE_JSON_PROPERTY_VALUE, InputDependency.TYPE_JSON_PROPERTY_VALUE,
                ConfiguredAtlasFetcher.TYPE_JSON_PROPERTY_VALUE,
                ConfiguredSubAtlas.TYPE_JSON_PROPERTY_VALUE,
                ConfiguredMergeForgivenessPolicy.TYPE_JSON_PROPERTY_VALUE,
                ConfiguredBroadcastable.TYPE_JSON_PROPERTY_VALUE));

        /*
         * Add the names of all the various configuration objects. These include country codes,
         * mutator names, DynamicAtlas policy names, etc.
         */
        allKeys.addAll(this.configuration.getCountryToMutationLevels().keySet());
        allKeys.addAll(this.mutators.keySet());
        allKeys.addAll(this.dynamicAtlasPolicies.keySet());
        allKeys.addAll(this.filters.keySet());
        allKeys.addAll(this.inputDependencies.keySet());
        allKeys.addAll(this.fetchers.keySet());
        allKeys.addAll(this.subAtlases.keySet());
        allKeys.addAll(this.mergeForgivenessPolicies.keySet());
        allKeys.addAll(this.broadcastables.keySet());

        return allKeys;
    }

    public Map<String, ConfiguredBroadcastable> getBroadcastables()
    {
        return this.broadcastables;
    }

    public Map<String, ConfiguredDynamicAtlasPolicy> getDynamicAtlasPolicies()
    {
        return this.dynamicAtlasPolicies;
    }

    public Map<String, ConfiguredAtlasFetcher> getFetchers()
    {
        return this.fetchers;
    }

    public Map<String, ConfiguredFilter> getFilters()
    {
        return this.filters;
    }

    public Map<String, InputDependency> getInputDependencies()
    {
        return this.inputDependencies;
    }

    public Map<String, ConfiguredMergeForgivenessPolicy> getMergeForgivenessPolicies()
    {
        return this.mergeForgivenessPolicies;
    }

    public Map<String, ConfiguredAtlasChangeGenerator> getMutators()
    {
        return this.mutators;
    }

    public Map<String, ConfiguredSubAtlas> getSubAtlases()
    {
        return this.subAtlases;
    }

    private void discoverExtendedConfigurationMaps() // NOSONAR
    {
        for (final Map.Entry<String, List<AtlasMutationLevel>> entry : this.configuration
                .getCountryToMutationLevels().entrySet())
        {
            for (final AtlasMutationLevel level : entry.getValue())
            {
                /*
                 * Discover the dynamicAtlasPolicies.
                 */
                final ConfiguredDynamicAtlasPolicy generationPolicy = level
                        .getConfiguredGenerationPolicy();
                final ConfiguredDynamicAtlasPolicy applicationPolicy = level
                        .getConfiguredApplicationPolicy();
                if (!this.dynamicAtlasPolicies.containsKey(generationPolicy.getName()))
                {
                    this.dynamicAtlasPolicies.put(generationPolicy.getName(), generationPolicy);
                }
                if (!this.dynamicAtlasPolicies.containsKey(applicationPolicy.getName()))
                {
                    this.dynamicAtlasPolicies.put(applicationPolicy.getName(), applicationPolicy);
                }

                /*
                 * Discover the mutators.
                 */
                for (final ConfiguredAtlasChangeGenerator mutator : level.getMutators())
                {
                    if (!this.mutators.containsKey(mutator.getName()))
                    {
                        this.mutators.put(mutator.getName(), mutator);
                    }
                }
            }
        }

        for (final Map.Entry<String, ConfiguredDynamicAtlasPolicy> entry : this.dynamicAtlasPolicies
                .entrySet())
        {
            /*
             * Discover the filters.
             */
            final ConfiguredFilter filter = entry.getValue().getEntitiesToConsiderForExpansion();
            if (filter != null)
            {
                this.filters.put(filter.getName(), filter);
            }

            /*
             * Discover input dependencies.
             */
            entry.getValue().getInputDependency()
                    .ifPresent(inputDependency -> this.inputDependencies
                            .put(inputDependency.getPathName(), inputDependency));
            /*
             * Discover the fetchers.
             */
            final ConfiguredAtlasFetcher fetcher = entry.getValue().getConfiguredFetcher();
            if (fetcher != null)
            {
                this.fetchers.put(fetcher.getName(), fetcher);
            }
        }

        for (final Map.Entry<String, ConfiguredAtlasChangeGenerator> entry : this.mutators
                .entrySet())
        {
            /*
             * Discover more input dependencies.
             */
            for (final InputDependency dependency : entry.getValue().getInputDependencies())
            {
                if (!this.inputDependencies.containsKey(dependency.getPathName()))
                {
                    this.inputDependencies.put(dependency.getPathName(), dependency);
                }
            }

            /*
             * Discover mergeForgivenessPolicies.
             */
            final ConfiguredMergeForgivenessPolicy policy = entry.getValue()
                    .getMergeForgivenessPolicy();
            if (policy != null && !this.mergeForgivenessPolicies.containsKey(policy.getOwner())
                    && !policy.policyIsEmpty())
            {
                this.mergeForgivenessPolicies.put(policy.getOwner(), policy);
            }

            /*
             * Discover broadcastables.
             */
            final Map<String, ConfiguredBroadcastable> broadcastVariables = entry.getValue()
                    .getBroadcastVariablesNeeded();
            if (broadcastVariables != null)
            {
                this.broadcastables.putAll(broadcastVariables);
            }
        }

        for (final Map.Entry<String, InputDependency> entry : this.inputDependencies.entrySet())
        {
            /*
             * Discover sub atlases.
             */
            final ConfiguredSubAtlas subAtlas = entry.getValue().getSubAtlas();
            if (subAtlas != null)
            {
                this.subAtlases.put(subAtlas.getName(), subAtlas);
            }
        }

        for (final Map.Entry<String, ConfiguredAtlasFetcher> entry : this.fetchers.entrySet())
        {
            /*
             * Discover more sub atlases.
             */
            final ConfiguredSubAtlas subAtlas = entry.getValue().getSubAtlas();
            if (subAtlas != null && !this.subAtlases.containsKey(subAtlas.getName()))
            {
                this.subAtlases.put(subAtlas.getName(), subAtlas);
            }
        }

        /*
         * Discover the global mergeForgivenessPolicy.
         */
        this.mergeForgivenessPolicies.put(
                this.configuration.getGlobalMergeForgivenessPolicy().getOwner(),
                this.configuration.getGlobalMergeForgivenessPolicy());

    }
}
