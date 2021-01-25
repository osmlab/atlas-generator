package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGeneratorBuilder;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.graphs.DirectedAcyclicGraph;
import org.openstreetmap.atlas.utilities.maps.MultiMapWithSet;
import org.openstreetmap.atlas.utilities.tuples.Tuple;

/**
 * This is a helper class that parses a configuration object to derive all the levels in order for
 * each country.
 *
 * @author matthieun
 */
public class AtlasMutatorConfigurationParser
{
    public static final String CONFIGURATION_GLOBAL = "global";

    private final AtlasMutatorConfiguration atlasMutatorConfiguration;
    private final ConfiguredAtlasChangeGeneratorBuilder configuredAtlasChangeGeneratorBuilder;
    private final Configuration mutatorsConfiguration;

    static Map<String, List<AtlasMutationLevel>> coalesce(
            final Map<String, List<AtlasMutationLevel>> countryToMutationLevels)
    {
        final AtomicInteger identifierGenerator = new AtomicInteger();
        final Map<String, List<AtlasMutationLevel>> groups = new HashMap<>();
        for (final Entry<String, List<AtlasMutationLevel>> entry : countryToMutationLevels
                .entrySet())
        {
            String keyToRemove = null;
            Tuple<String, List<AtlasMutationLevel>> entryToAdd = null;
            for (final Entry<String, List<AtlasMutationLevel>> groupEntry : groups.entrySet())
            {
                final List<AtlasMutationLevel> merged = isSimilar(identifierGenerator,
                        groupEntry.getValue(), entry.getValue(), groupEntry.getKey());
                if (!merged.isEmpty())
                {
                    keyToRemove = groupEntry.getKey();
                    final String key = merged.get(0).getCountryGroup();
                    entryToAdd = new Tuple<>(key, merged);
                    break;
                }
            }
            if (keyToRemove != null)
            {
                groups.remove(keyToRemove);
                groups.put(entryToAdd.getFirst(), entryToAdd.getSecond());
            }
            else
            {
                groups.put(entry.getKey(), entry.getValue());
            }
        }
        return groups;
    }

    private static List<AtlasMutationLevel> isSimilar(final AtomicInteger identifierGenerator,
            final List<AtlasMutationLevel> levelList1, final List<AtlasMutationLevel> levelList2,
            final String groupNameOption)
    {
        final List<AtlasMutationLevel> result = new ArrayList<>();
        if (levelList1.size() != levelList2.size())
        {
            return result;
        }
        for (int index = 0; index < levelList1.size(); index++)
        {
            if (!levelList1.get(index).isSimilarTo(levelList2.get(index)))
            {
                return result;
            }
        }
        final String groupName;
        if (groupNameOption.startsWith(AtlasMutatorConfiguration.GROUP_PREFIX))
        {
            groupName = groupNameOption;
        }
        else
        {
            groupName = AtlasMutatorConfiguration.GROUP_PREFIX
                    + identifierGenerator.getAndIncrement();
        }
        for (int index = 0; index < levelList1.size(); index++)
        {
            result.add(levelList1.get(index).merge(levelList2.get(index), groupName));
        }
        return result;
    }

    public AtlasMutatorConfigurationParser(
            final AtlasMutatorConfiguration atlasMutatorConfiguration,
            final Configuration mutatorsConfiguration, final Map<String, String> sparkConfiguration)
    {
        this.atlasMutatorConfiguration = atlasMutatorConfiguration;
        this.mutatorsConfiguration = mutatorsConfiguration;
        this.configuredAtlasChangeGeneratorBuilder = new ConfiguredAtlasChangeGeneratorBuilder(
                mutatorsConfiguration, sparkConfiguration);
    }

    /**
     * @param countries
     *            All the countries that are expected to be mutated
     * @param groupCountries
     *            Whether or not to attempt to group countries together
     * @param allowRDD
     *            Whether levels are allowed to request the previous level to provide AtlasRDD for
     *            their input
     * @param preloadRDD
     *            Whether or not to allow eligible mutation levels to preload atlas data into an RDD
     *            before processing mutations
     * @return A map of country code to an ordered list of {@link AtlasMutationLevel} for each
     *         country code.
     */
    public Map<String, List<AtlasMutationLevel>> generateCountryToMutationLevels(
            final Set<String> countries, final boolean groupCountries, final boolean allowRDD,
            final boolean preloadRDD)
    {
        final AtlasMutatorIndex index = new AtlasMutatorIndex(
                this.configuredAtlasChangeGeneratorBuilder);
        final Set<String> mutatorNames = this.mutatorsConfiguration.configurationDataKeySet()
                .stream().filter(value -> !CONFIGURATION_GLOBAL.equals(value))
                .collect(Collectors.toSet());
        index.populate(countries, mutatorNames, this.mutatorsConfiguration);

        final Map<String, DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator>> countryToDagMap = new HashMap<>();
        countries.forEach(country ->
        {
            final DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator> actionableDag = index
                    .getActionableDag(country);
            // Add only if the DAG is not empty
            if (!actionableDag.getSources().isEmpty())
            {
                countryToDagMap.put(country, index.getActionableDag(country));
            }
        });

        final Map<String, List<AtlasMutationLevel>> result = new HashMap<>();
        for (final Entry<String, DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator>> countryToDagMapEntry : countryToDagMap
                .entrySet())
        {
            final String country = countryToDagMapEntry.getKey();
            final DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator> dag = countryToDagMapEntry
                    .getValue();
            final List<Set<ConfiguredAtlasChangeGenerator>> atlasChangeGeneratorsGroups = splitAccordingToDynamicAtlasPolicies(
                    dag.processGroups(), dag);
            final int maximumLevelIndex = atlasChangeGeneratorsGroups.size() - 1;
            int levelIndex = 0;
            final List<AtlasMutationLevel> levels = new ArrayList<>();
            for (final Set<ConfiguredAtlasChangeGenerator> generatorGroup : atlasChangeGeneratorsGroups)
            {
                final AtlasMutationLevel level = new AtlasMutationLevel(
                        this.atlasMutatorConfiguration, country, Sets.hashSet(country),
                        generatorGroup, levelIndex, maximumLevelIndex);
                level.setAllowRDD(allowRDD);
                level.setPreloadRDD(preloadRDD);
                levels.add(level);
                if (levelIndex > 0)
                {
                    final AtlasMutationLevel parent = levels.get(levelIndex - 1);
                    // There is a parent Level to which this level needs to tell what filtered
                    // output to provide. Also it needs to know if the parent level is RDD based.
                    level.notifyOfParentLevel(parent);
                    parent.notifyOfChildLevel(level);
                }
                levelIndex++;
            }
            result.put(country, levels);
        }
        if (groupCountries)
        {
            return coalesce(result);
        }
        else
        {
            return result;
        }
    }

    /**
     * @param mutator
     *            The {@link ConfiguredAtlasChangeGenerator} that is a candidate to be moved earlier
     *            in the dag
     * @param levelIndex
     *            The int that represents the level the mutator would be moved to
     * @param result
     *            An {@link ArrayList} that holds all the {@link ConfiguredAtlasChangeGenerator}s
     *            that have been processed so far
     * @param dag
     *            The {@link DirectedAcyclicGraph} that is being split by dynamic atlas policies
     * @return True if the mutators parents have been processed before the levelIndex, otherwise
     *         false
     */
    private boolean parentsAlreadyProcessedBeforeThisLevel(
            final ConfiguredAtlasChangeGenerator mutator, final int levelIndex,
            final List<Set<ConfiguredAtlasChangeGenerator>> result,
            final DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator> dag)
    {
        final Set<ConfiguredAtlasChangeGenerator> parents = dag.getParents(mutator);
        final List<ConfiguredAtlasChangeGenerator> allProcessed = new ArrayList<>();
        // collect all mutators that have been added before the given level
        for (int i = 0; i < levelIndex; i++)
        {
            final Set<ConfiguredAtlasChangeGenerator> level = result.get(i);
            allProcessed.addAll(level);
        }
        return allProcessed.containsAll(parents);
    }

    /**
     * Make sure that each set of mutators all have the same generation and application dynamic
     * atlas policies
     *
     * @param atlasChangeGeneratorsGroups
     *            the un-split groups
     * @return Groups of mutators that are split to make sure all have the same generation and
     *         application dynamic atlas policies within each set.
     */
    private List<Set<ConfiguredAtlasChangeGenerator>> splitAccordingToDynamicAtlasPolicies(
            final List<Set<ConfiguredAtlasChangeGenerator>> atlasChangeGeneratorsGroups,
            final DirectedAcyclicGraph<ConfiguredAtlasChangeGenerator> dag)
    {
        final List<Set<ConfiguredAtlasChangeGenerator>> result = new ArrayList<>();
        final Set<ConfiguredAtlasChangeGenerator> sources = dag.getSources();
        final HashMap<Tuple<ConfiguredDynamicAtlasPolicy, ConfiguredDynamicAtlasPolicy>, Integer> expansionPolicyToLevelIndex = new HashMap<>();
        int levelIndex = 0;
        for (final Set<ConfiguredAtlasChangeGenerator> generatorGroup : atlasChangeGeneratorsGroups)
        {
            final MultiMapWithSet<Tuple<ConfiguredDynamicAtlasPolicy, ConfiguredDynamicAtlasPolicy>, ConfiguredAtlasChangeGenerator> generationApplicationToMutator = new MultiMapWithSet<>();
            // group subgroups by their dynamic atlas expansion policies
            for (final ConfiguredAtlasChangeGenerator mutator : generatorGroup)
            {
                final Tuple<ConfiguredDynamicAtlasPolicy, ConfiguredDynamicAtlasPolicy> generationApplicationTuple = new Tuple<>(
                        mutator.getDynamicAtlasPolicyGeneration(),
                        mutator.getDynamicAtlasPolicyApplication());
                final Integer matchingDynamicPolicyLevelIndex = expansionPolicyToLevelIndex
                        .get(generationApplicationTuple);
                // check levels we've already added to result to see if the mutator could be
                // added there instead. Mutator can be moved earlier if it is a source, or if all
                // it's parents have been processed before the matching level
                if (matchingDynamicPolicyLevelIndex == null)
                {
                    generationApplicationToMutator.add(generationApplicationTuple, mutator);
                }
                else
                {
                    if (sources.contains(mutator) || parentsAlreadyProcessedBeforeThisLevel(mutator,
                            matchingDynamicPolicyLevelIndex, result, dag))
                    {
                        final Set<ConfiguredAtlasChangeGenerator> mutationsOnLevel = result
                                .get(matchingDynamicPolicyLevelIndex);
                        mutationsOnLevel.add(mutator);
                        result.set(matchingDynamicPolicyLevelIndex, mutationsOnLevel);
                    }
                    else
                    {
                        generationApplicationToMutator.add(generationApplicationTuple, mutator);
                    }
                }
            }
            // Make sure that the ordering is always deterministic
            final SortedSet<Set<ConfiguredAtlasChangeGenerator>> sortedMutatorGroups = new TreeSet<>(
                    (setLeft, setRight) ->
                    {
                        final SortedSet<String> namesLeft = setLeft.stream()
                                .map(ConfiguredAtlasChangeGenerator::getName)
                                .collect(Collectors.toCollection(TreeSet::new));
                        final SortedSet<String> namesRight = setRight.stream()
                                .map(ConfiguredAtlasChangeGenerator::getName)
                                .collect(Collectors.toCollection(TreeSet::new));
                        final StringBuilder nameLeft = new StringBuilder();
                        namesLeft.forEach(nameLeft::append);
                        final StringBuilder nameRight = new StringBuilder();
                        namesRight.forEach(nameRight::append);
                        return nameLeft.toString().compareTo(nameRight.toString());
                    });
            sortedMutatorGroups.addAll(generationApplicationToMutator.values());

            // add each new subgroup to the result
            for (final Set<ConfiguredAtlasChangeGenerator> mutatorGroup : sortedMutatorGroups)
            {
                result.add(mutatorGroup);
                final ConfiguredAtlasChangeGenerator mutator = mutatorGroup.iterator().next();
                // all mutators in a mutatorGroup will have the same dynamic atlas policy, so we can
                // just grab the first one (also guaranteed to not be empty)
                final Tuple<ConfiguredDynamicAtlasPolicy, ConfiguredDynamicAtlasPolicy> generationApplicationTuple = new Tuple<>(
                        mutator.getDynamicAtlasPolicyGeneration(),
                        mutator.getDynamicAtlasPolicyApplication());
                expansionPolicyToLevelIndex.put(generationApplicationTuple, levelIndex);
                levelIndex++;
            }
        }
        return result;
    }
}
