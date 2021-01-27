package org.openstreetmap.atlas.mutator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.InputDependency;
import org.openstreetmap.atlas.mutator.persistence.FeatureChangeOutputFormat;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.OutputStreamWritableResource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.tuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * @author matthieun
 */
public class AtlasMutatorUntangled extends Command
{
    /**
     * @author matthieun
     */
    private static class LocalFeatureChangeOutputFormat extends FeatureChangeOutputFormat
    {
        @Override
        protected void save(final List<FeatureChange> value, final AbstractWritableResource out) // NOSONAR
        {
            super.save(value, out);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AtlasMutatorUntangled.class);
    private static final Switch<Integer> LEVEL_INDEX = new Switch<>("levelIndex",
            "The level index to run for that country.", Integer::parseInt, Optionality.OPTIONAL,
            "0");
    private static final Switch<Shard> SHARD = new Switch<>("shard",
            "The shard to run for that country.", SlippyTile::forName, Optionality.REQUIRED);
    private static final Switch<Boolean> RUN_ALL_LEVELS_UP_TO = new Switch<>("runAllLevelsUpTo",
            "If true, this will run all the levels until the specified level for the specified shard.",
            Boolean::parseBoolean, Optionality.OPTIONAL, "false");
    // TODO implement support for ConfiguredMergeForgivenessStrategy?

    private static final String ERROR = "Error";

    private Map<String, String> sparkConfiguration = Maps.hashMap();

    public static void main(final String[] args)
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
        new AtlasMutatorUntangled().runWithoutQuitting(args);
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final List<AtlasMutationLevel> levels = getLevels(command);
        final Integer levelIndex = (Integer) command.get(LEVEL_INDEX);
        final Shard shard = (Shard) command.get(SHARD);
        final boolean runAllLevelsUpTo = (boolean) command.get(RUN_ALL_LEVELS_UP_TO);
        forceFeedBroadcastVariables(levels);

        for (int index = 0; index <= levelIndex; index++)
        {
            if (runAllLevelsUpTo || index == levelIndex)
            {
                final AtlasMutationLevel runnableLevel = levels.get(index);
                AtlasMutatorParameters.debugMutations(command)
                        .forEach(runnableLevel::addDebugWhiteListedMutator);
                start(runnableLevel,
                        new CountryShard(runnableLevel.getCountries().iterator().next(), shard));
            }
        }
        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return AtlasMutatorParameters.switches().with(LEVEL_INDEX, SHARD, RUN_ALL_LEVELS_UP_TO);
    }

    protected AtlasMutatorUntangled withSparkConfiguration(
            final Map<String, String> sparkConfiguration)
    {
        this.sparkConfiguration = sparkConfiguration;
        return this;
    }

    private void forceFeedBroadcastVariables(final List<AtlasMutationLevel> levels)
    {
        final Map<String, Object> broadcastVariables = new HashMap<>();
        levels.forEach(level -> level.getMutators()
                .forEach(mutator -> mutator.getBroadcastVariablesNeeded().forEach((name, value) ->
                {
                    if (!broadcastVariables.containsKey(name))
                    {
                        broadcastVariables.put(name, value.read(this.sparkConfiguration));
                    }
                })));
        levels.forEach(level -> level.getMutators()
                .forEach(mutator -> broadcastVariables.forEach(mutator::addBroadcastVariable)));
    }

    private String getCountry(final CommandMap command)
    {
        final StringList countries = (StringList) command.get(AtlasGeneratorParameters.COUNTRIES);
        if (countries.size() != 1)
        {
            throw new CoreException("Number of countries has to be exactly 1.");
        }
        return countries.get(0);
    }

    private AtlasMutationLevel getLevel(final CommandMap command)
    {
        final Integer levelIndex = (Integer) command.get(LEVEL_INDEX);
        if (levelIndex < 0)
        {
            throw new CoreException("levelIndex has to be >= 0");
        }
        final String country = getCountry(command);

        final List<AtlasMutationLevel> levels = getLevels(command);
        if (levels.size() <= levelIndex)
        {
            throw new CoreException("Country {} mutation levels do not contain index {}", country,
                    levelIndex);
        }
        final AtlasMutationLevel level = levels.get(levelIndex);
        if (level.getLevelIndex() != levelIndex)
        {
            throw new CoreException(
                    "Country {} mutation levels are malformed. Expected level index {} but found level index {} instead.",
                    country, levelIndex, level.getLevelIndex());
        }
        return level;
    }

    private List<AtlasMutationLevel> getLevels(final CommandMap command)
    {
        final String country = getCountry(command);
        final AtlasMutatorConfiguration atlasMutatorConfiguration = AtlasMutatorParameters
                .atlasMutatorConfiguration(command, this.sparkConfiguration);
        if (logger.isInfoEnabled())
        {
            logger.info("AtlasMutator country and levels:\n\n{}\n",
                    atlasMutatorConfiguration.details());
        }
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = atlasMutatorConfiguration
                .getCountryToMutationLevels();
        if (!countryToMutationLevels.containsKey(country))
        {
            throw new CoreException("Configuration does not contain country {}", country);
        }
        return countryToMutationLevels.get(country);
    }

    private Configuration hadoopConfiguration()
    {
        final Configuration result = new Configuration();
        this.sparkConfiguration.forEach(result::set);
        return result;
    }

    private AbstractWritableResource outputResource(final AtlasMutationLevel level,
            final CountryShard countryShard, final String type)
    {
        return outputResource(SparkFileHelper.parentPath(level.getOutputAtlasPath()) + "/logs/"
                + countryShard.getCountry() + "-" + level.getLevelIndex() + "-" + type + "/"
                + countryShard.getCountry() + "-" + level.getLevelIndex() + "-" + type + "_"
                + countryShard.getShard().getName() + FileSuffix.GEO_JSON.toString());
    }

    private AbstractWritableResource outputResource(final String path)
    {
        try
        {
            return new OutputStreamWritableResource(
                    new Path(path).getFileSystem(hadoopConfiguration()).create(new Path(path)));
        }
        catch (IllegalArgumentException | IOException e)
        {
            throw new CoreException("Unable to write to {}", path, e);
        }
    }

    /**
     * This is the same as {@link AtlasMutator}.start
     *
     * @param level
     *            The level to run
     * @param countryShard
     *            The shard to run
     */
    private void start(final AtlasMutationLevel level, final CountryShard countryShard) // NOSONAR
    {
        // 1. Get all the feature changes for a shard (This includes 2. Filter out the ones that are
        // too far from the shard (dynamicAtlas side effects))
        final List<Tuple2<CountryShard, List<FeatureChange>>> shardFeatureChangesRDD;
        try
        {
            shardFeatureChangesRDD = Lists.newArrayList(
                    AtlasMutatorHelper.shardToFeatureChanges(level).call(countryShard));
        }
        catch (final Exception e)
        {
            throw new CoreException(ERROR, e);
        }

        // Save generated logs
        shardFeatureChangesRDD.forEach(tuple ->
        {
            final CountryShard countryShardInternal = tuple._1();
            final AbstractWritableResource outputResource = outputResource(level,
                    countryShardInternal, "generated");
            new LocalFeatureChangeOutputFormat().save(tuple._2(), outputResource);
        });

        // 3. Assign feature changes to the shards where the change can be applied
        final List<Tuple2<CountryShard, List<FeatureChange>>> shardAssignedFeatureChangesRDD0 = shardFeatureChangesRDD
                .stream().flatMap(tuple ->
                {
                    try
                    {
                        return Lists.newArrayList(AtlasMutatorHelper
                                .shardFeatureChangesToAssignedShardFeatureChanges(level)
                                .call(tuple)).stream();
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException(ERROR, e);
                    }
                }).collect(Collectors.toList());

        // 4. Merge all feature changes of each shard into a single list (and resolve conflicts)
        final List<Tuple2<CountryShard, List<FeatureChange>>> shardAssignedFeatureChangesRDD1 = shardAssignedFeatureChangesRDD0
                .stream()
                // For the untangled only, we care about applying back to one shard only
                .filter(tuple -> countryShard.equals(tuple._1())).collect(Collectors.toList());
        // concatenate in a fat list
        final Map<CountryShard, List<FeatureChange>> shardAssignedFeatureChangesRDD2 = new HashMap<>();
        for (final Tuple2<CountryShard, List<FeatureChange>> shardToList : shardAssignedFeatureChangesRDD1)
        {
            final CountryShard shardInternal = shardToList._1();
            final List<FeatureChange> value = shardToList._2();
            if (shardAssignedFeatureChangesRDD2.containsKey(shardInternal))
            {
                try
                {
                    shardAssignedFeatureChangesRDD2.put(shardInternal,
                            AtlasMutatorHelper.assignedToConcatenatedFeatureChanges(level).call(
                                    shardAssignedFeatureChangesRDD2.get(shardInternal), value));
                }
                catch (final Exception e)
                {
                    throw new CoreException(ERROR, e);
                }
            }
            else
            {
                shardAssignedFeatureChangesRDD2.put(shardInternal, value);
            }
        }

        // Save assigned logs
        shardAssignedFeatureChangesRDD2.forEach((countryShardInternal, featureChanges) ->
        {
            final AbstractWritableResource outputResource = outputResource(level,
                    countryShardInternal, "assigned");
            new LocalFeatureChangeOutputFormat().save(featureChanges, outputResource);
        });

        // Trim by merging colliding featureChanges
        final List<Tuple2<CountryShard, List<FeatureChange>>> shardAppliedFeatureChangesRDD = shardAssignedFeatureChangesRDD2
                .entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                .map(tuple ->
                {
                    try
                    {
                        return AtlasMutatorHelper.assignedToShardAppliedFeatureChanges(level)
                                .call(tuple);
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException(ERROR, e);
                    }
                }).collect(Collectors.toList());

        // 5. Make a Change with the List of FeatureChange
        final List<Tuple2<CountryShard, Change>> shardChangeRDD = shardAppliedFeatureChangesRDD
                .stream().map(tuple ->
                {
                    try
                    {
                        return AtlasMutatorHelper.featureChangeListToChange(level).call(tuple);
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException(ERROR, e);
                    }
                }).collect(Collectors.toList());

        // 6. Apply the change to the proper DynamicAtlas
        final List<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> changedAtlasRDD = shardChangeRDD
                .stream().flatMap(tuple ->
                {
                    try
                    {
                        return Lists
                                .newArrayList(AtlasMutatorHelper.changeToAtlas(level).call(tuple))
                                .stream();
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException(ERROR, e);
                    }
                }).collect(Collectors.toList());

        // Save applied logs
        changedAtlasRDD.stream().filter(tuple -> tuple._2().getSecond() != null).forEach(tuple ->
        {
            final CountryShard countryShardInternal = tuple._1();
            final AbstractWritableResource outputResource = outputResource(level,
                    countryShardInternal, "applied");
            new LocalFeatureChangeOutputFormat().save(tuple._2().getSecond().getFeatureChanges(),
                    outputResource);
        });

        // 7. Backfill all the shards that have no Change applied
        final Set<CountryShard> populatedShards = changedAtlasRDD.stream().map(
                // Here we need to map to String then read back to Shard to avoid an un-serializable
                // lambda error.
                tuple -> tuple._1().getName()).map(CountryShard::forName)
                .collect(Collectors.toSet());

        final List<Tuple2<CountryShard, PackedAtlas>> unChangedAtlasRDD = Sets.hashSet(countryShard)
                .stream().filter(shardInternal -> !populatedShards.contains(shardInternal))
                .flatMap(shardInternal ->
                {
                    try
                    {
                        return Lists.newArrayList(
                                AtlasMutatorHelper.untouchedShardToPotentialSourcePackedAtlas(level)
                                        .call(shardInternal))
                                .stream();
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException(ERROR, e);
                    }
                }).collect(Collectors.toList());
        final List<Tuple2<CountryShard, PackedAtlas>> allUpdatedAtlasShardsRDD = new ArrayList<>();
        allUpdatedAtlasShardsRDD
                .addAll(changedAtlasRDD.stream().filter(tuple -> tuple._2().getSecond() != null)
                        .map(tuple -> new Tuple2<>(tuple._1(), tuple._2().getFirst()))
                        .collect(Collectors.toList()));
        allUpdatedAtlasShardsRDD.addAll(unChangedAtlasRDD);

        // 8. Save the cloned ChangeAtlas
        allUpdatedAtlasShardsRDD.forEach(tuple ->
        {
            final CountryShard countryShardInternal = tuple._1();
            final String country = countryShardInternal.getCountry();
            tuple._2().save(outputResource(level.getOutputAtlasPath() + "/" + country + "/"
                    + countryShard.getName() + ".atlas"));
        });

        // 9. Save any potential input dependencies to provide
        for (final InputDependency inputDependency : level.getInputDependenciesToProvide())
        {
            allUpdatedAtlasShardsRDD.stream().flatMap(tuple ->
            {
                try
                {
                    return Lists.newArrayList(AtlasMutatorHelper
                            .inputDependencyFilteredAtlas(level, inputDependency).call(tuple))
                            .stream();
                }
                catch (final Exception e)
                {
                    throw new CoreException(ERROR, e);
                }
            }).forEach(tuple ->
            {
                final CountryShard countryShardInternal = tuple._1();
                final String country = countryShardInternal.getCountry();
                tuple._2().save(outputResource(level.getOutputAtlasPath(inputDependency) + "/"
                        + country + "/" + countryShard.getName() + ".atlas"));
            });
        }
    }
}
