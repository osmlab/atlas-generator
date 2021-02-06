package org.openstreetmap.atlas.mutator.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.broadcast.Broadcast;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.dynamic.policy.DynamicAtlasPolicy;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.boundary.CountryShardListing;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.mutator.AtlasMutator;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.parsing.ConfiguredAtlasFetcher;
import org.openstreetmap.atlas.mutator.configuration.parsing.ConfiguredDynamicAtlasPolicy;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.maps.MultiMapWithSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Class that holds level and country specific configuration for an {@link AtlasMutator}
 *
 * @author matthieun
 */
public class AtlasMutationLevel implements Serializable
{
    public static final String COUNTRY_INDEX_SEPARATOR = "_";
    public static final String INTERMEDIATE_FOLDER_NAME = "intermediate";

    public static final String TYPE_JSON_PROPERTY_VALUE = "_level";
    public static final String COUNTRY_OR_GROUP_JSON_PROPERTY = "countryOrGroup";
    public static final String GROUP_CONTENTS_JSON_PROPERTY = "groupContents";
    public static final String LEVEL_INDEX_JSON_PROPERTY = "levelIndex";
    public static final String LEVEL_SOURCE_TYPE_JSON_PROPERTY = "levelSourceType";
    public static final String MUTATORS_JSON_PROPERTY = "mutators";
    public static final String INPUT_DEPENDENCIES_PROVIDE_JSON_PROPERTY = "inputDependenciesToProvide";
    public static final String INPUT_DEPENDENCIES_REQUEST_JSON_PROPERTY = "inputDependenciesToRequest";
    public static final String DYNAMIC_ATLAS_POLICY_GENERATION_JSON_PROPERTY = "dynamicAtlasPolicyGeneration";
    public static final String DYNAMIC_ATLAS_POLICY_APPLICATION_JSON_PROPERTY = "dynamicAtlasPolicyApplication";
    public static final String PARENT_NEEDS_RDD_INPUT_JSON_PROPERTY = "parentNeedsRDDInput";
    public static final String CHILD_NEEDS_RDD_INPUT_JSON_PROPERTY = "childNeedsRDDInput";
    public static final String CHILD_CAN_PRELOAD_RDD_INPUT_JSON_PROPERTY = "childCanPreloadRDDInput";
    public static final String MAX_LEVEL_INDEX_JSON_PROPERTY = "maximumLevelIndex";
    public static final String ADD_MUTATION_TAGS_JSON_PROPERTY = "addMutationTags";

    private static final long serialVersionUID = 2921374291315186276L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasMutationLevel.class);

    private final AtlasMutatorConfiguration atlasMutatorConfiguration;
    private final String countryGroup;
    private final Set<String> countries;
    private final MultiMapWithSet<Shard, String> shardsToCountries;
    private final Set<ConfiguredAtlasChangeGenerator> mutators;
    private final int levelIndex;
    private final int maximumLevelIndex;
    private final Set<InputDependency> inputDependenciesToRequest;
    private final ConfiguredDynamicAtlasPolicy dynamicAtlasPolicyGeneration;
    private final ConfiguredDynamicAtlasPolicy dynamicAtlasPolicyApplication;
    private final Set<InputDependency> inputDependenciesToProvide;
    private final Set<String> debugIncludeListedMutators = new HashSet<>();
    private final Set<String> debugIncludeListedShards = new HashSet<>();
    private boolean addMutationTags;
    private boolean allowRDD = false;
    private boolean preloadRDD = false;
    private final Map<String, Broadcast<?>> broadcastVariables;
    private boolean parentNeedsRDDInput = false;
    private boolean childNeedsRDDInput = false;
    private boolean childCanPreloadRDDInput = false;

    private static String getName(final String country, final int levelIndex)
    {
        return country + COUNTRY_INDEX_SEPARATOR + levelIndex;
    }

    public AtlasMutationLevel(final AtlasMutatorConfiguration atlasMutatorConfiguration,
            final String countryGroup, final Set<String> countries,
            final Set<ConfiguredAtlasChangeGenerator> mutators, final int levelIndex,
            final int maximumLevelIndex)
    {
        this.atlasMutatorConfiguration = atlasMutatorConfiguration;
        this.countryGroup = countryGroup;
        this.countries = countries;
        this.shardsToCountries = new MultiMapWithSet<>();
        this.mutators = mutators;
        this.levelIndex = levelIndex;
        this.maximumLevelIndex = maximumLevelIndex;
        this.inputDependenciesToRequest = inputDependenciesToRequest(mutators);
        this.dynamicAtlasPolicyGeneration = dynamicAtlasPolicy(mutators,
                ConfiguredAtlasChangeGenerator::getDynamicAtlasPolicyGeneration);
        this.dynamicAtlasPolicyApplication = dynamicAtlasPolicy(mutators,
                ConfiguredAtlasChangeGenerator::getDynamicAtlasPolicyApplication);
        this.inputDependenciesToProvide = new HashSet<>();
        this.addMutationTags = true;
        this.broadcastVariables = new HashMap<>();
    }

    public void addBroadcastVariable(final String name, final Broadcast<?> broadcast)
    {
        this.broadcastVariables.put(name, broadcast);
    }

    public void addDebugWhiteListedMutator(final String name)
    {
        this.debugIncludeListedMutators.add(name);
    }

    public void addDebugWhiteListedShard(final String name)
    {
        this.debugIncludeListedShards.add(name);
    }

    public boolean canPreloadAtlasRDD()
    {
        return
        // Preloading RDD must be allowed
        this.preloadRDD && canSourceAtlasObjectsFromRDD();
    }

    public boolean canRequestSourceAtlasObjectsFromRDD()
    {
        return
        // First level cannot request an RDD from a previous level
        this.levelIndex > 0
                // Make sure previous level was not RDD. This is to avoid Spark stage leakage from
                // level to level.
                && !this.parentNeedsRDDInput
                // Make sure the level can physically source from RDD
                && canSourceAtlasObjectsFromRDD();
    }

    public boolean canSourceAtlasObjectsFromRDD()
    {
        return
        // Make sure that the DynamicAtlasPolicies are deterministic
        this.dynamicAtlasPolicyGeneration.isPreDetermined()
                && this.dynamicAtlasPolicyApplication.isPreDetermined();
    }

    public String details()
    {
        final StringBuilder result = new StringBuilder();
        result.append(this);
        if (this.requestSourceAtlasObjectsFromRDD())
        {
            result.append(": RDDLEVEL,");
        }
        else if (this.canPreloadAtlasRDD())
        {
            result.append(": RDDFS,");
        }
        else
        {
            result.append(": FS,");
        }
        final StringList mutations = new StringList();
        this.mutators.stream().map(ConfiguredAtlasChangeGenerator::toString).sorted()
                .forEach(mutations::add);
        result.append(mutations.join(","));
        return result.toString();
    }

    @Override
    public boolean equals(final Object other)
    {
        if (other instanceof AtlasMutationLevel)
        {
            return other.toString().equals(this.toString());
        }
        return false;
    }

    public Optional<InputDependency> getApplicationInputDependencyToRequest()
    {
        return this.dynamicAtlasPolicyApplication.getInputDependency();
    }

    public DynamicAtlasPolicy getApplicationPolicy(final CountryShard countryShard)
    {
        return getPolicy(countryShard, this.dynamicAtlasPolicyApplication, "an application");
    }

    public Function<CountryShard, Set<CountryShard>> getApplicationShardExplorer()
    {
        return getShardExplorer(this.dynamicAtlasPolicyApplication);
    }

    public AtlasMutatorConfiguration getAtlasMutatorConfiguration()
    {
        return this.atlasMutatorConfiguration;
    }

    public Map<String, Object> getBroadcastVariables()
    {
        final Map<String, Object> result = new HashMap<>();
        for (final Map.Entry<String, Broadcast<?>> entry : this.broadcastVariables.entrySet())
        {
            try
            {
                result.put(entry.getKey(), entry.getValue().getValue());
            }
            catch (final Exception e)
            {
                throw new CoreException("{}: Unable to read broadcast variable \"{}\"", this,
                        entry.getKey(), e);
            }
        }
        return result;
    }

    public ConfiguredDynamicAtlasPolicy getConfiguredApplicationPolicy()
    {
        return this.dynamicAtlasPolicyApplication;
    }

    public ConfiguredDynamicAtlasPolicy getConfiguredGenerationPolicy()
    {
        return this.dynamicAtlasPolicyGeneration;
    }

    public Set<String> getCountries()
    {
        return this.countries;
    }

    public String getCountryGroup()
    {
        return this.countryGroup;
    }

    public Set<String> getDebugIncludeListedMutators()
    {
        return this.debugIncludeListedMutators;
    }

    public Set<String> getDebugIncludeListedShards()
    {
        return this.debugIncludeListedShards;
    }

    public Optional<InputDependency> getGenerationInputDependencyToRequest()
    {
        return this.dynamicAtlasPolicyGeneration.getInputDependency();
    }

    public DynamicAtlasPolicy getGenerationPolicy(final CountryShard countryShard)
    {
        return getPolicy(countryShard, this.dynamicAtlasPolicyGeneration, "a generation");
    }

    public Function<CountryShard, Set<CountryShard>> getGenerationShardExplorer()
    {
        return getShardExplorer(this.dynamicAtlasPolicyGeneration);
    }

    public Set<InputDependency> getInputDependenciesToProvide()
    {
        return ImmutableSet.copyOf(this.inputDependenciesToProvide);
    }

    public Set<InputDependency> getInputDependenciesToRequest()
    {
        return ImmutableSet.copyOf(this.inputDependenciesToRequest);
    }

    public int getLevelIndex()
    {
        return this.levelIndex;
    }

    public String getLevelSourceType()
    {
        if (this.requestSourceAtlasObjectsFromRDD())
        {
            return "RDDLEVEL";
        }
        else if (this.canPreloadAtlasRDD())
        {
            return "RDDFS";
        }
        return "FS";
    }

    public int getMaximumLevelIndex()
    {
        return this.maximumLevelIndex;
    }

    public Set<ConfiguredAtlasChangeGenerator> getMutators()
    {
        return this.mutators;
    }

    public String getOutputAtlasPath()
    {
        final String outputPath;
        if (this.levelIndex == this.maximumLevelIndex)
        {
            outputPath = this.atlasMutatorConfiguration.getOutput();
        }
        else
        {
            outputPath = getIntermediateOutput(this.atlasMutatorConfiguration.getOutput());
        }
        return outputPath;
    }

    public String getOutputAtlasPath(final InputDependency inputDependency)
    {
        return getIntermediateOutput(this.atlasMutatorConfiguration.getOutput(),
                this + InputDependency.INPUT_DEPENDENCY_FOLDER_KEY + inputDependency.getPathName());
    }

    public DynamicAtlasPolicy getRDDBasedApplicationPolicy(final CountryShard countryShard,
            final Map<CountryShard, PackedAtlas> shardToAtlasMap)
    {
        return getRDDBasedPolicy(this.dynamicAtlasPolicyApplication, countryShard, shardToAtlasMap,
                "an application");
    }

    public DynamicAtlasPolicy getRDDBasedGenerationPolicy(final CountryShard countryShard,
            final Map<CountryShard, PackedAtlas> shardToAtlasMap)
    {
        return getRDDBasedPolicy(this.dynamicAtlasPolicyGeneration, countryShard, shardToAtlasMap,
                "a generation");
    }

    public MultiMapWithSet<Shard, String> getShardsToCountries()
    {
        return this.shardsToCountries;
    }

    public Function<CountryShard, Optional<Atlas>> getSourceFetcher()
    {
        return countryShard -> ConfiguredAtlasFetcher.direct()
                .getFetcher(getParentAtlasPath(), countryShard.getCountry(),
                        this.atlasMutatorConfiguration.getSparkConfiguration())
                .apply(countryShard.getShard());
    }

    public String groupAndIndex()
    {
        return getName(this.countryGroup, this.levelIndex);
    }

    @Override
    public int hashCode()
    {
        return this.toString().hashCode();
    }

    public boolean isAddMutationTags()
    {
        return this.addMutationTags;
    }

    /**
     * @return True when the next level has a dynamic atlas policy deterministic enough where it
     *         could read input Atlas data from a pre-loaded RDD of Atlas, or from the current
     *         level's result atlas RDD.
     */
    public boolean isChildCanPreloadRDDInput()
    {
        return this.childCanPreloadRDDInput;
    }

    /**
     * @return True when the next level will need the current level's result RDD to read from
     */
    public boolean isChildNeedsRDDInput()
    {
        return this.childNeedsRDDInput;
    }

    /**
     * @return True when there are no levels after this one.
     */
    public boolean isLast()
    {
        return this.levelIndex == this.maximumLevelIndex;
    }

    /**
     * @return True if the level before this one already read its input from its previous level's
     *         output RDD.
     */
    public boolean isParentNeedsRDDInput()
    {
        return this.parentNeedsRDDInput;
    }

    public boolean isSimilarTo(final AtlasMutationLevel other)
    {
        return this.getLevelIndex() == other.getLevelIndex()
                && this.getMaximumLevelIndex() == other.getMaximumLevelIndex()
                && this.getMutators().equals(other.getMutators())
                && this.getInputDependenciesToRequest()
                        .equals(other.getInputDependenciesToRequest())
                && this.getInputDependenciesToProvide()
                        .equals(other.getInputDependenciesToProvide())
                && this.dynamicAtlasPolicyApplication.equals(other.dynamicAtlasPolicyApplication)
                && this.dynamicAtlasPolicyGeneration.equals(other.dynamicAtlasPolicyGeneration);
    }

    public AtlasMutationLevel merge(final AtlasMutationLevel other, final String groupName)
    {
        if (!isSimilarTo(other))
        {
            throw new CoreException("Cannot merge {} and {} which are not similar.", this, other);
        }
        final Set<String> mergedCountries = new HashSet<>();
        mergedCountries.addAll(this.countries);
        mergedCountries.addAll(other.countries);
        final AtlasMutationLevel result = new AtlasMutationLevel(this.atlasMutatorConfiguration,
                groupName, mergedCountries, this.mutators, this.levelIndex, this.maximumLevelIndex);
        result.addInputDependenciesToProvide(this.getInputDependenciesToProvide());
        result.childNeedsRDDInput = this.childNeedsRDDInput;
        result.childCanPreloadRDDInput = this.childCanPreloadRDDInput;
        result.parentNeedsRDDInput = this.parentNeedsRDDInput;
        result.allowRDD = this.allowRDD;
        result.preloadRDD = this.preloadRDD;
        return result;
    }

    public void notifyOfChildLevel(final AtlasMutationLevel childLevel)
    {
        this.childNeedsRDDInput = childLevel.requestSourceAtlasObjectsFromRDD();
        this.childCanPreloadRDDInput = childLevel.canPreloadAtlasRDD();
    }

    /**
     * @param parentLevel
     *            The parent level to notify.
     */
    public void notifyOfParentLevel(final AtlasMutationLevel parentLevel)
    {
        parentLevel.addInputDependenciesToProvide(this.inputDependenciesToRequest);
        this.parentNeedsRDDInput = parentLevel.requestSourceAtlasObjectsFromRDD();
    }

    public boolean requestSourceAtlasObjectsFromRDD()
    {
        return
        // From the switches. Always off if allowRDD is false.
        this.allowRDD
                // Make sure the level is allowed to request source from RDD
                && canRequestSourceAtlasObjectsFromRDD();
    }

    public void setAddMutationTags(final boolean addMutationTags)
    {
        this.addMutationTags = addMutationTags;
    }

    public void setAllowRDD(final boolean allowRDD)
    {
        this.allowRDD = allowRDD;
    }

    public void setPreloadRDD(final boolean preloadRDD)
    {
        this.preloadRDD = preloadRDD;
    }

    public void setShardsToCountries(final List<CountryShard> countryShardsList)
    {
        countryShardsList.forEach(countryShard -> this.shardsToCountries
                .add(countryShard.getShard(), countryShard.getCountry()));
    }

    public List<CountryShard> shards()
    {
        final MultiMapWithSet<String, Shard> countryToShardMap = CountryShardListing
                .countryToShardList(this.countries, this.atlasMutatorConfiguration.getBoundaries(),
                        this.atlasMutatorConfiguration.getSharding());
        return countryToShardMap.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(shard -> new CountryShard(entry.getKey(), shard)))
                .collect(Collectors.toList());
    }

    public JsonObject toJson()
    {
        final JsonObject levelJson = new JsonObject();

        levelJson.addProperty(AtlasMutatorConfiguration.TYPE_JSON_PROPERTY,
                TYPE_JSON_PROPERTY_VALUE);

        levelJson.addProperty(COUNTRY_OR_GROUP_JSON_PROPERTY, this.countryGroup);
        if (!this.countries.isEmpty())
        {
            levelJson.addProperty(GROUP_CONTENTS_JSON_PROPERTY,
                    new StringList(this.countries).join(","));
        }

        levelJson.addProperty(LEVEL_INDEX_JSON_PROPERTY, this.levelIndex);
        levelJson.addProperty(LEVEL_SOURCE_TYPE_JSON_PROPERTY, this.getLevelSourceType());

        final JsonArray mutatorsArray = new JsonArray();
        for (final String mutatorName : new TreeSet<>(this.mutators.stream()
                .map(ConfiguredAtlasChangeGenerator::getName).collect(Collectors.toSet())))
        {
            mutatorsArray.add(new JsonPrimitive(mutatorName));
        }
        levelJson.add(MUTATORS_JSON_PROPERTY, mutatorsArray);

        if (!this.inputDependenciesToProvide.isEmpty())
        {
            levelJson.addProperty(INPUT_DEPENDENCIES_PROVIDE_JSON_PROPERTY,
                    new StringList(this.getInputDependenciesToProvide().stream()
                            .map(InputDependency::toString).collect(Collectors.toList()))
                                    .join(","));
        }
        if (!this.inputDependenciesToRequest.isEmpty())
        {
            levelJson.addProperty(INPUT_DEPENDENCIES_REQUEST_JSON_PROPERTY,
                    new StringList(this.getInputDependenciesToRequest().stream()
                            .map(InputDependency::toString).collect(Collectors.toList()))
                                    .join(","));
        }

        levelJson.addProperty(DYNAMIC_ATLAS_POLICY_GENERATION_JSON_PROPERTY,
                this.dynamicAtlasPolicyGeneration.toStringCompact());
        levelJson.addProperty(DYNAMIC_ATLAS_POLICY_APPLICATION_JSON_PROPERTY,
                this.dynamicAtlasPolicyApplication.toStringCompact());
        levelJson.addProperty(PARENT_NEEDS_RDD_INPUT_JSON_PROPERTY, this.parentNeedsRDDInput);
        levelJson.addProperty(CHILD_NEEDS_RDD_INPUT_JSON_PROPERTY, this.childNeedsRDDInput);
        levelJson.addProperty(CHILD_CAN_PRELOAD_RDD_INPUT_JSON_PROPERTY,
                this.childCanPreloadRDDInput);
        levelJson.addProperty(MAX_LEVEL_INDEX_JSON_PROPERTY, this.maximumLevelIndex);
        levelJson.addProperty(ADD_MUTATION_TAGS_JSON_PROPERTY, this.addMutationTags);

        return levelJson;
    }

    @Override
    public String toString()
    {
        return getName(this.countryGroup, this.levelIndex);
    }

    private void addInputDependenciesToProvide(final Set<InputDependency> inputDependencies)
    {
        this.inputDependenciesToProvide.addAll(inputDependencies);
        this.validateInputDependencyGroup(this.inputDependenciesToProvide);
    }

    private Map<Shard, PackedAtlas> backToShard(
            final Map<CountryShard, PackedAtlas> countryShardToAtlasMap)
    {
        final Map<Shard, PackedAtlas> result = new HashMap<>();
        countryShardToAtlasMap.forEach(
                (countryShard, packedAtlas) -> result.put(countryShard.getShard(), packedAtlas));
        return result;
    }

    private ConfiguredDynamicAtlasPolicy dynamicAtlasPolicy(
            final Set<ConfiguredAtlasChangeGenerator> mutators,
            final Function<ConfiguredAtlasChangeGenerator, ConfiguredDynamicAtlasPolicy> mutatorToPolicy)
    {
        final Set<ConfiguredDynamicAtlasPolicy> result = mutators.stream().map(mutatorToPolicy)
                .collect(Collectors.toSet());
        if (result.isEmpty())
        {
            throw new CoreException("{}: Cannot have zero dynamic atlas policy per level.", this);
        }
        if (result.size() > 1)
        {
            throw new CoreException("{}: Cannot have more than one dynamic atlas policy per level.",
                    this);
        }
        return result.iterator().next();
    }

    private String getIntermediateOutput(final String output, final int index)
    {
        return getIntermediateOutput(output, getName(this.countryGroup, index));
    }

    private String getIntermediateOutput(final String output, final String built)
    {
        return SparkFileHelper.combine(output, INTERMEDIATE_FOLDER_NAME, built);
    }

    private String getIntermediateOutput(final String output)
    {
        return getIntermediateOutput(output, this.levelIndex);
    }

    private String getParentAtlasPath()
    {
        final String parentPath;
        if (this.levelIndex == 0)
        {
            parentPath = this.atlasMutatorConfiguration.getInput();
        }
        else
        {
            final int index = this.levelIndex - 1;
            parentPath = getIntermediateOutput(this.atlasMutatorConfiguration.getOutput(), index);
        }
        return parentPath;
    }

    private DynamicAtlasPolicy getPolicy(final CountryShard countryShard,
            final ConfiguredDynamicAtlasPolicy configuredDynamicAtlasPolicy, final String message)
    {
        if (this.canSourceAtlasObjectsFromRDD())
        {
            // If a Level can source from RDD does not mean it has to. (for example when debugging
            // locally)
            logger.warn("{} could use {} policy that is RDD Based", this, message);
        }
        return configuredDynamicAtlasPolicy.withShardsToCountries(this.shardsToCountries).getPolicy(
                Sets.hashSet(countryShard.getShard()), this.atlasMutatorConfiguration.getSharding(),
                getParentAtlasPath(), this.atlasMutatorConfiguration.getSparkConfiguration(),
                countryShard.getCountry());
    }

    private DynamicAtlasPolicy getRDDBasedPolicy(
            final ConfiguredDynamicAtlasPolicy configuredDynamicAtlasPolicy,
            final CountryShard countryShard, final Map<CountryShard, PackedAtlas> shardToAtlasMap,
            final String message)
    {
        if (!this.canSourceAtlasObjectsFromRDD())
        {
            throw new CoreException("{} should not use {} policy that is RDD Based", this, message);
        }
        return configuredDynamicAtlasPolicy.getRDDBasedPolicy(Sets.hashSet(countryShard.getShard()),
                this.atlasMutatorConfiguration.getSharding(), backToShard(shardToAtlasMap));
    }

    private Function<CountryShard, Set<CountryShard>> getShardExplorer(
            final ConfiguredDynamicAtlasPolicy policy)
    {
        return (Serializable & Function<CountryShard, Set<CountryShard>>) countryShard -> policy
                .getShardExplorer(this.atlasMutatorConfiguration.getSharding())
                .apply(countryShard.getShard()).stream()
                .map(shard -> new CountryShard(countryShard.getCountry(), shard))
                .collect(Collectors.toSet());
    }

    private Set<InputDependency> inputDependenciesToRequest(
            final Set<ConfiguredAtlasChangeGenerator> mutators)
    {
        final Set<InputDependency> result = new HashSet<>();
        for (final ConfiguredAtlasChangeGenerator mutator : mutators)
        {
            result.addAll(mutator.getInputDependencies());
        }
        validateInputDependencyGroup(result);
        return result;
    }

    private void validateInputDependencyGroup(final Set<InputDependency> result)
    {
        final List<InputDependency> internal = new ArrayList<>(result);
        // Validate no path conflicts
        for (int index = 0; index < result.size(); index++)
        {
            for (int jndex = index + 1; jndex < result.size(); jndex++)
            {
                final InputDependency dependencyI = internal.get(index);
                final InputDependency dependencyJ = internal.get(jndex);
                if (!dependencyI.getPathName().equals(dependencyJ.getPathName()))
                {
                    throw new CoreException(
                            "Two input dependency list for {} and {} collided on the path {}",
                            dependencyI.getOwner(), dependencyJ.getOwner(),
                            dependencyI.getPathName());
                }
            }
        }
    }
}
