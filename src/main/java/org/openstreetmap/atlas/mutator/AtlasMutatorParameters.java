package org.openstreetmap.atlas.mutator;

import java.io.Serializable;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.Partitioner;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.generator.tools.spark.persistence.PersistenceTools;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.streaming.resource.http.GetResource;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.MergedConfiguration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.runtime.Command.Optionality;
import org.openstreetmap.atlas.utilities.runtime.Command.Switch;
import org.openstreetmap.atlas.utilities.runtime.Command.SwitchList;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.scalars.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class helps configure an {@link AtlasMutator} by defining and parsing its {@link Switch}es.
 *
 * @author matthieun
 */
public final class AtlasMutatorParameters
{
    /**
     * Options to output debug ldgeojson logs in {@link AtlasMutator}
     *
     * @author matthieun
     */
    public enum DebugFeatureChangeOutput
    {
        ALL,
        APPLIED,
        NONE
    }

    public static final Switch<StringList> MUTATOR_CONFIGURATION_RESOURCE = new Switch<>(
            "mutatorConfigurationResource", "Comma-separated list of configuration resources.",
            value -> StringList.split(value, ","), Optionality.OPTIONAL);
    public static final Switch<String> MUTATOR_CONFIGURATION_JSON = new Switch<>(
            "mutatorConfigurationJson", "Json formatted configuration.", StringConverter.IDENTITY,
            Optionality.OPTIONAL);
    public static final Switch<String> INPUT_ATLAS = new Switch<>("atlas",
            "Path to the input Atlas folder", StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<String> SHARDING_PROVIDED = new Switch<>("sharding",
            "Provided sharding tree to override the one from the Atlas folder. Use at your own risk!"
                    + " If they do not match with the input folder, the job's behavior is undefined.",
            StringConverter.IDENTITY, Optionality.OPTIONAL, "");
    public static final Switch<String> BOUNDARIES_PROVIDED = new Switch<>("boundaries",
            "Provided boundary file to override the one from the Atlas folder. Use at your own risk!"
                    + " If they do not match with the input folder, the job's behavior is undefined.",
            StringConverter.IDENTITY, Optionality.OPTIONAL, "");
    public static final Switch<Integer> COUNTRY_CONCURRENCY = new Switch<>("countryConcurrency",
            "The number of countries that are submitted to the Spark Cluster at the same time.",
            Integer::parseInt, Optionality.OPTIONAL, "300");
    public static final Switch<Boolean> ADD_MUTATION_TAGS = new Switch<>("addMutationTags",
            "Whether or not to add a tag to each feature touched by a mutation with the mutation name.",
            Boolean::parseBoolean, Optionality.OPTIONAL, "true");
    public static final Switch<Boolean> ALLOW_RDD = new Switch<>("allowRDD",
            "Whether or not to allow eligible mutation levels to source data from previous level's RDD",
            Boolean::parseBoolean, Optionality.OPTIONAL, "false");
    public static final Switch<Duration> SUBMISSION_STAGGERING_WAIT = new Switch<>(
            "submissionStaggeringWaitInSeconds",
            "How long to wait between each country submission to the driver",
            value -> Duration.seconds(Double.parseDouble(value)), Optionality.OPTIONAL, "0.0");
    public static final Switch<Boolean> GROUP_COUNTRIES = new Switch<>("groupCountries",
            "When true, this allows the driver to group countries together by similarity in levels.",
            Boolean::parseBoolean, Optionality.OPTIONAL, "true");
    public static final Switch<Boolean> PRELOAD_RDD = new Switch<>("preloadRDD",
            "Whether or not to allow eligible mutation levels to preload atlas data into an RDD before processing mutations",
            Boolean::parseBoolean, Optionality.OPTIONAL, "false");

    /*
     * Optional debugging parameters. These work best when used for local runs.
     */
    public static final Switch<Integer> DEBUG_SKIP_TO_LEVEL = new Switch<>("debugSkipToLevel",
            "What level to skip to", Integer::parseInt, Optionality.OPTIONAL, "0");
    public static final Switch<Integer> DEBUG_STOP_AT_LEVEL = new Switch<>("debugStopAtLevel",
            "What level to stop at", Integer::parseInt, Optionality.OPTIONAL,
            String.valueOf(Integer.MAX_VALUE));
    public static final Switch<Boolean> DEBUG_APPLIED_RDD_PERSIST_USE_SER = new Switch<>(
            "debugAppliedRDDPersistUseSer",
            "When persisting shardAppliedFeatureChangesRDD, if true use MEMORY_AND_DISK_SER. If false, uses MEMORY_AND_DISK",
            Boolean::parseBoolean, Optionality.OPTIONAL, "false");
    private static final StringConverter<StringList> DEBUG_CSV = value -> value.isEmpty()
            ? new StringList()
            : StringList.split(value, ",");
    public static final Switch<StringList> DEBUG_SHARDS = new Switch<>("debugShards",
            "Comma separated list of shards of interest", DEBUG_CSV, Optionality.OPTIONAL, "");
    public static final Switch<StringList> DEBUG_MUTATIONS = new Switch<>("debugMutations",
            "Comma separated list of mutations of interest", DEBUG_CSV, Optionality.OPTIONAL, "");
    private static final Logger logger = LoggerFactory.getLogger(AtlasMutatorParameters.class);
    public static final Switch<DebugFeatureChangeOutput> DEBUG_FEATURE_CHANGE_OUTPUT = new Switch<>(
            "debugFeatureChangeOutput",
            "Whether to save every stage of the feature changes. " + "Values are \""
                    + DebugFeatureChangeOutput.ALL + "\", \"" + DebugFeatureChangeOutput.APPLIED
                    + "\", or \"" + DebugFeatureChangeOutput.NONE + "\"",
            value ->
            {
                try
                {
                    return DebugFeatureChangeOutput.valueOf(value.toUpperCase());
                }
                catch (final IllegalArgumentException e)
                {
                    logger.error("{} \"{}\" is not recognized! Using {} instead.",
                            DebugFeatureChangeOutput.class.getSimpleName(), value,
                            DebugFeatureChangeOutput.APPLIED);
                    return DebugFeatureChangeOutput.APPLIED;
                }
            }, Optionality.OPTIONAL, DebugFeatureChangeOutput.APPLIED.name());

    public static CountryBoundaryMap boundaries(final CommandMap command,
            final Map<String, String> configuration)
    {
        final String providedBoundaries = (String) command.get(BOUNDARIES_PROVIDED);
        if (!providedBoundaries.isEmpty())
        {
            return CountryBoundaryMap
                    .fromPlainText(SparkJob.resource(providedBoundaries, configuration));
        }
        final String input = input(command);
        return new PersistenceTools(configuration).boundaries(input);
    }

    public static Set<String> countries(final CommandMap command)
    {
        return ((StringList) command.get(AtlasGeneratorParameters.COUNTRIES)).stream()
                .collect(Collectors.toSet());
    }

    public static Integer countryConcurrency(final CommandMap command)
    {
        return (Integer) command.get(COUNTRY_CONCURRENCY);
    }

    public static DebugFeatureChangeOutput debugFeatureChangeOutput(final CommandMap command)
    {
        return (DebugFeatureChangeOutput) command.get(DEBUG_FEATURE_CHANGE_OUTPUT);
    }

    /**
     * Load the {@link Configuration} for this job using the job parameters and supplied spark
     * configuration.
     *
     * @param command
     *            the job parameters {@link CommandMap}
     * @param sparkConfiguration
     *            the spark configuration
     * @return the job {@link Configuration} object
     */
    public static Configuration mutatorsConfiguration(final CommandMap command,
            final Map<String, String> sparkConfiguration)
    {
        final Optional<Configuration> configurationOptional = mutatorsConfiguration(
                (StringList) command.get(AtlasMutatorParameters.MUTATOR_CONFIGURATION_RESOURCE),
                (String) command.get(AtlasMutatorParameters.MUTATOR_CONFIGURATION_JSON),
                sparkConfiguration);
        final Configuration configuration;
        if (configurationOptional.isPresent())
        {
            configuration = configurationOptional.get();
        }
        else
        {
            throw new CoreException("The configuration is empty!");
        }
        return configuration;
    }

    /**
     * Load the {@link Configuration} for this job explicitly, by providing a list of resources and
     * optional JSON overrides. Also, use the supplied {@link FileSystem} to load any local path
     * resources.
     *
     * @param resources
     *            a {@link StringList} of resources containing configurations
     * @param json
     *            a JSON string of configuration overrides
     * @param sourceFileSystem
     *            the {@link FileSystem} from which to load local configuration paths
     * @return the job {@link Configuration} object
     */
    public static Optional<Configuration> mutatorsConfiguration(final StringList resources,
            final String json, final FileSystem sourceFileSystem)
    {
        return mutatorsConfiguration(resources, json, null, sourceFileSystem);
    }

    /**
     * Load the {@link Configuration} for this job explicitly, by providing a list of resources and
     * optional JSON overrides. Also, use the supplied Spark configuration {@link Map} to load any
     * local path resources with {@link SparkJob#resource(String, Map)}.
     *
     * @param resources
     *            a {@link StringList} of resources containing configurations
     * @param json
     *            a JSON string of configuration overrides
     * @param sparkConfiguration
     *            the spark configuration for this job
     * @return the job {@link Configuration} object
     */
    public static Optional<Configuration> mutatorsConfiguration(final StringList resources,
            final String json, final Map<String, String> sparkConfiguration)
    {
        return mutatorsConfiguration(resources, json, sparkConfiguration, null);
    }

    public static Comparator<CountryShard> shardComparator()
    {
        return (Serializable & Comparator<CountryShard>) (shard1, shard2) -> shard1.getName()
                .compareTo(shard2.getName());
    }

    /**
     * {@link Partitioner} to be used in a repartition() call.
     *
     * @param countryShards
     *            The shards to use to do the partitioning
     * @return A {@link Partitioner} that can take an RDD with shard keys and make sure each shard
     *         goes to a different partition.
     */
    public static Partitioner shardPartitioner(final Set<CountryShard> countryShards)
    {
        final Map<CountryShard, Integer> shardToIndex = new HashMap<>();
        int index = 0;
        for (final CountryShard countryShard : countryShards)
        {
            shardToIndex.put(countryShard, index);
            index++;
        }
        return new Partitioner()
        {
            private static final long serialVersionUID = -779349575361610965L;

            @Override
            public int getPartition(final Object object)
            {
                if (object instanceof CountryShard)
                {
                    return shardToIndex.get(object);
                }
                throw new CoreException("{} is not a CountryShard.", object);
            }

            @Override
            public int numPartitions()
            {
                return countryShards.size();
            }
        };
    }

    public static Sharding sharding(final CommandMap command,
            final Map<String, String> configuration)
    {
        final String providedSharding = (String) command.get(SHARDING_PROVIDED);
        if (!providedSharding.isEmpty())
        {
            return AtlasSharding.forString(providedSharding, configuration);
        }
        final String input = input(command);
        try
        {
            return new PersistenceTools(configuration).sharding(input);
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to find sharding tree in {}", input, e);
        }
    }

    public static Duration submissionStaggering(final CommandMap command)
    {
        return (Duration) command.get(SUBMISSION_STAGGERING_WAIT);
    }

    protected static AtlasMutatorConfiguration atlasMutatorConfiguration(final CommandMap command,
            final Map<String, String> sparkConfiguration)
    {
        final String input = input(command);
        final String output = output(command);
        final boolean groupCountries = isGroupCountries(command);
        final boolean allowRDD = isAllowRDD(command);
        final boolean preloadRDD = isPreloadRDD(command);
        return new AtlasMutatorConfiguration(countries(command),
                sharding(command, sparkConfiguration), boundaries(command, sparkConfiguration),
                input, output, sparkConfiguration,
                mutatorsConfiguration(command, sparkConfiguration), groupCountries, allowRDD,
                preloadRDD);
    }

    protected static StringList debugMutations(final CommandMap command)
    {
        return (StringList) command.get(DEBUG_MUTATIONS);
    }

    protected static StringList debugShards(final CommandMap command)
    {
        return (StringList) command.get(DEBUG_SHARDS);
    }

    protected static boolean isAddMutationTags(final CommandMap command)
    {
        return (boolean) command.get(ADD_MUTATION_TAGS);
    }

    protected static boolean isAllowRDD(final CommandMap command)
    {
        return (boolean) command.get(ALLOW_RDD);
    }

    protected static boolean isAppliedRDDPersistUseSer(final CommandMap command)
    {
        return (boolean) command.get(DEBUG_APPLIED_RDD_PERSIST_USE_SER);
    }

    protected static boolean isGroupCountries(final CommandMap command)
    {
        return (boolean) command.get(GROUP_COUNTRIES);
    }

    protected static boolean isPreloadRDD(final CommandMap command)
    {
        return (boolean) command.get(PRELOAD_RDD);
    }

    protected static int startLevel(final CommandMap command)
    {
        final int result = (int) command.get(DEBUG_SKIP_TO_LEVEL);
        if (result < 0)
        {
            throw new CoreException("Debug Start Level {} has to be > 0", result);
        }
        return result;
    }

    protected static int stopLevel(final CommandMap command)
    {
        final int result = (int) command.get(DEBUG_STOP_AT_LEVEL);
        if (result < 0)
        {
            throw new CoreException("Debug Stop Level {} has to be > 0", result);
        }
        return result;
    }

    protected static SwitchList switches()
    {
        return new SwitchList().with(AtlasGeneratorParameters.COUNTRIES,
                AtlasGeneratorParameters.CODE_VERSION, AtlasGeneratorParameters.DATA_VERSION,
                AtlasGeneratorParameters.ATLAS_SCHEME, MUTATOR_CONFIGURATION_JSON,
                MUTATOR_CONFIGURATION_RESOURCE, INPUT_ATLAS, SHARDING_PROVIDED, BOUNDARIES_PROVIDED,
                COUNTRY_CONCURRENCY, DEBUG_FEATURE_CHANGE_OUTPUT, DEBUG_SKIP_TO_LEVEL,
                DEBUG_STOP_AT_LEVEL, DEBUG_SHARDS, DEBUG_MUTATIONS,
                DEBUG_APPLIED_RDD_PERSIST_USE_SER, PersistenceTools.COPY_SHARDING_AND_BOUNDARIES,
                ADD_MUTATION_TAGS, ALLOW_RDD, SUBMISSION_STAGGERING_WAIT, GROUP_COUNTRIES,
                PRELOAD_RDD);
    }

    static String input(final CommandMap command)
    {
        return (String) command.get(INPUT_ATLAS);
    }

    static String output(final CommandMap command)
    {
        return (String) command.get(SparkJob.OUTPUT);
    }

    private static List<Configuration> loadConfigurationsFromResourcePath(final String resourcePath,
            final Map<String, String> sparkConfiguration, final FileSystem sourceFileSystem)
    {
        final List<Configuration> mutatorConfigurations = new ArrayList<>();
        if (resourcePath.startsWith("http"))
        {
            mutatorConfigurations.add(new StandardConfiguration(new GetResource(resourcePath)));
        }
        else
        {
            if (sparkConfiguration != null)
            {
                mutatorConfigurations.add(new StandardConfiguration(
                        SparkJob.resource(resourcePath, sparkConfiguration)));
                logger.debug("Loaded configuration using SparkJob.resource");
            }
            else if (sourceFileSystem != null)
            {
                mutatorConfigurations
                        .add(new StandardConfiguration(new File(resourcePath, sourceFileSystem)));
                logger.debug("Loaded configuration using FileSystem");
            }
            else
            {
                throw new CoreException(
                        "sparkConfiguration and sourceFileSystem cannot both be null!");
            }
        }
        return mutatorConfigurations;
    }

    private static Optional<Configuration> mutatorsConfiguration(final StringList resources,
            final String json, final Map<String, String> sparkConfiguration,
            final FileSystem sourceFileSystem)
    {
        final List<Configuration> mutatorConfigurations = new ArrayList<>();

        // General configuration
        if (resources != null && !resources.isEmpty())
        {
            for (final String resourcePath : resources)
            {
                mutatorConfigurations.addAll(loadConfigurationsFromResourcePath(resourcePath,
                        sparkConfiguration, sourceFileSystem));
            }
        }

        if (json != null && !json.isEmpty())
        {
            // Make sure this configuration always be the first one which will overwrite following
            // configurations that has the same key
            mutatorConfigurations.add(0, new StandardConfiguration(new StringResource(json)));
        }

        if (mutatorConfigurations.isEmpty())
        {
            return Optional.empty();
        }

        return Optional.of(new MergedConfiguration(mutatorConfigurations));
    }

    private AtlasMutatorParameters()
    {
    }
}
