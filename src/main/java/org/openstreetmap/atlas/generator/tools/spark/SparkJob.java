package org.openstreetmap.atlas.generator.tools.spark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.UnaryOperator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.JavaSerializer;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemPerformanceHelper;
import org.openstreetmap.atlas.generator.tools.spark.context.DefaultSparkContextProvider;
import org.openstreetmap.atlas.generator.tools.spark.context.SparkContextProvider;
import org.openstreetmap.atlas.generator.tools.spark.context.SparkContextProviderFinder;
import org.openstreetmap.atlas.generator.tools.spark.converters.SparkOptionsStringConverter;
import org.openstreetmap.atlas.generator.tools.spark.persistence.PersistenceTools;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.streaming.compression.Decompressor;
import org.openstreetmap.atlas.streaming.resource.AbstractResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.http.GetResource;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Skeleton for a Spark Job
 *
 * @author matthieun
 */
public abstract class SparkJob extends Command implements Serializable
{
    public static final Switch<String> INPUT = new Switch<>("input", "Input path of the Spark Job",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> OUTPUT = new Switch<>("output",
            "Output path of the Spark Job", StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<String> CLUSTER = new Switch<>("cluster", "The spark cluster URL",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<Map<String, String>> SPARK_OPTIONS = new Switch<>("sparkOptions",
            "Comma separated list of Spark options, i.e. key1->value1,key2->value2",
            new SparkOptionsStringConverter(), Optionality.OPTIONAL, "");
    public static final Switch<Map<String, String>> ADDITIONAL_SPARK_OPTIONS = new Switch<>(
            "additionalSparkOptions",
            "Comma separated list of additional Spark options, i.e. key1->value1,key2->value2",
            new SparkOptionsStringConverter(), Optionality.OPTIONAL, "");
    public static final Switch<String> COMPRESS_OUTPUT = new Switch<>("compressOutput",
            "Whether or not compress the output of spark job", StringConverter.IDENTITY,
            Optionality.OPTIONAL, "true");
    public static final Switch<SparkContextProvider> SPARK_CONTEXT_PROVIDER = new Switch<>(
            "sparkContextProvider", "The class name of the Spark Context Provider",
            new SparkContextProviderFinder(), Optionality.OPTIONAL,
            DefaultSparkContextProvider.class.getCanonicalName());

    public static final String SUCCESS_FILE = "_SUCCESS";
    public static final String FAILED_FILE = "_FAILED";
    public static final String SAVING_SEPARATOR = "-";

    private static final long serialVersionUID = -3267868312907886517L;
    private static final Logger logger = LoggerFactory.getLogger(SparkJob.class);

    private transient JavaSparkContext context;

    public static Resource resource(final String path, final Map<String, String> configurationMap)
    {
        try
        {
            if (path.startsWith("http"))
            {
                return new GetResource(path);
            }
            final FileSystem fileSystem = new FileSystemCreator().get(path, configurationMap);
            if (!fileSystem.exists(new Path(path)))
            {
                return null;
            }
            final AbstractResource resource = new InputStreamResource(() ->
            {
                try
                {
                    return fileSystem.open(new Path(path));
                }
                catch (IllegalArgumentException | IOException e)
                {
                    throw new CoreException("Unable to open {}", path, e);
                }
            });
            if (path.endsWith(FileSuffix.GZIP.toString()))
            {
                resource.setDecompressor(Decompressor.GZIP);
            }
            resource.setName(path);
            return resource;
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not open resource {}", path, e);
        }
    }

    /**
     * @return The name of the job
     */
    public abstract String getName();

    @Override
    public int onRun(final CommandMap command)
    {
        final String sparkCluster = (String) command.get(CLUSTER);
        @SuppressWarnings("unchecked")
        final Map<String, String> options = (Map<String, String>) command.get(SPARK_OPTIONS);
        @SuppressWarnings("unchecked")
        final Map<String, String> additionalOptions = (Map<String, String>) command
                .get(ADDITIONAL_SPARK_OPTIONS);
        // The additional options take precedence
        additionalOptions.forEach(options::put);

        // Initialize Spark
        final SparkConf configuration = new SparkConf().setAppName(getName());
        options.forEach(configuration::set);
        logOptions(options);

        if (sparkCluster != null)
        {
            // TODO replace with inclusive language once
            // https://issues.apache.org/jira/browse/SPARK-32333 is completed
            configuration.setMaster(sparkCluster);
        }
        configuration.set("spark.serializer", JavaSerializer.class.getCanonicalName());
        configuration.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS,
                (String) command.get(COMPRESS_OUTPUT));

        // Main function
        final String output = output(command);
        try (JavaSparkContext localcontext = ((SparkContextProvider) command
                .get(SPARK_CONTEXT_PROVIDER)).apply(configuration))
        {
            this.context = localcontext;
            // Add all the configuration items to the hadoop configuration too.
            final Configuration hadoopConfiguration = this.context.hadoopConfiguration();
            options.forEach(hadoopConfiguration::set);

            FileSystemPerformanceHelper.openRenamePool();
            start(command);
            FileSystemPerformanceHelper.waitForAndCloseRenamePool();
            writeStatus(output, SUCCESS_FILE, "Success!");
        }
        catch (final Exception e)
        {
            writeStatus(output, FAILED_FILE, "Failed!");
            throw new CoreException("Job {} failed.", getName(), e);
        }

        return 0;
    }

    /**
     * The spark Job
     *
     * @param command
     *            The arguments passed to the main method
     */
    public abstract void start(CommandMap command);

    protected Configuration configuration()
    {
        final Configuration result = new Configuration();
        for (final Tuple2<String, String> key : getContext().getConf().getAll())
        {
            result.set(key._1(), key._2());
        }
        return result;
    }

    protected Map<String, String> configurationMap()
    {
        final Map<String, String> result = new HashMap<>();
        for (final Tuple2<String, String> key : getContext().getConf().getAll())
        {
            result.put(key._1(), key._2());
        }
        return result;
    }

    protected void copyToOutput(final CommandMap command, final String input, final String output)
    {
        final Boolean copyToOutput = (Boolean) command
                .get(PersistenceTools.COPY_SHARDING_AND_BOUNDARIES);
        if (copyToOutput)
        {
            new PersistenceTools(configurationMap()).copyShardingAndBoundariesToOutput(input,
                    output);
        }
    }

    /**
     * Get an alternate output based on the main output folder used for monitoring
     *
     * @param output
     *            The main output folder
     * @param name
     *            The name of the alternate folder
     * @return The alternate output
     */
    protected String getAlternateParallelFolderOutput(final String output, final String name)
    {
        return SparkFileHelper.parentPath(output) + "-" + name;
    }

    /**
     * Get an alternate output based on the main output folder used for monitoring. Use the
     * sub-folder.
     *
     * @param output
     *            The main output folder
     * @param name
     *            The name of the alternate folder
     * @return The alternate output
     */
    protected String getAlternateSubFolderOutput(final String output, final String name)
    {
        return SparkFileHelper.combine(SparkFileHelper.parentPath(output), name);
    }

    protected JavaSparkContext getContext()
    {
        return this.context;
    }

    protected String input(final CommandMap command)
    {
        return (String) command.get(INPUT);
    }

    protected String output(final CommandMap command)
    {
        return (String) command.get(OUTPUT);
    }

    /**
     * Define all the folders to clean before a run.
     *
     * @param command
     *            The command parameters sent to the main class.
     * @return All the paths to clean
     */
    protected List<String> outputToClean(final CommandMap command)
    {
        final List<String> result = new ArrayList<>();
        result.add(output(command));
        return result;
    }

    /**
     * @param path
     *            The path to open (in an URL format)
     * @return The resource at this path
     */
    protected Resource resource(final String path)
    {
        return resource(path, configurationMap());
    }

    protected void setContext(final JavaSparkContext context)
    {
        this.context = context;
    }

    /**
     * Instead of saving a full RDD(String, T) in a single folder, this function allows to save
     * subsets of an RDD(String, T) in separate folders. The keyReducer function needs to provide
     * the unique String by which each key string needs to be grouped with. For example an RDD with
     * keys "aaa_1", "aaa_2", and "bbb_1" and a function that takes the first part of the key as a
     * grouping key will be saved in two different folders. If the path is /path/to/output, then the
     * two folders will be /path/to-aaa/ and /path/to-bbb/
     * <p>
     * This function might be slow as it will generate a Spark stage for each category in this RDD.
     * In the example above, it would create two stages. When the number of stages increases, it
     * might be really slow.
     *
     * @param <T>
     *            The type of the object to save
     * @param input
     *            The RDD to save
     * @param path
     *            The output path of the job
     * @param valueClass
     *            The type to save as Hadoop file
     * @param formatterClass
     *            The corresponding Hadoop formatter
     * @param keyReducer
     *            The key reducing function explained above.
     */
    protected <T> void splitAndSaveAsHadoopFile(final JavaPairRDD<String, T> input,
            final String path, final Class<T> valueClass,
            final Class<? extends MultipleOutputFormat<String, T>> formatterClass,
            final UnaryOperator<String> keyReducer)
    {
        // Get all the represented names.
        final List<String> splitNames = input.keys().map(keyReducer::apply).distinct().collect();
        // For split name, filter the existing RDD to only the specific name and save.
        for (final String splitName : splitNames)
        {
            input.filter(tuple -> splitName.equals(keyReducer.apply(tuple._1()))).saveAsHadoopFile(
                    path + SAVING_SEPARATOR + splitName, String.class, valueClass, formatterClass);
        }
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(INPUT, OUTPUT, CLUSTER, SPARK_OPTIONS,
                ADDITIONAL_SPARK_OPTIONS, COMPRESS_OUTPUT, SPARK_CONTEXT_PROVIDER);
    }

    private FileSystem getFileSystem(final String path) throws IOException
    {
        return new Path(path).getFileSystem(configuration());
    }

    private void logOptions(final Map<String, String> options)
    {
        if (logger.isInfoEnabled())
        {
            for (final Entry<String, String> entry : options.entrySet())
            {
                final String key = entry.getKey();
                final String value = entry.getValue();
                logger.info("Forcing configuration from -{}: key: \"{}\", value: \"{}\"",
                        SPARK_OPTIONS.getName(), key, value);
            }
        }
    }

    private void writeStatus(final String path, final String name, final String contents)
    {
        try
        {
            final FileSystem fileSystem = getFileSystem(path);
            try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                    fileSystem.create(new Path(SparkFileHelper.combine(path, name))))))
            {
                out.write(contents);
            }
        }
        catch (final Exception e)
        {
            logger.error("Could not write file {}/{}", path, name, e);
        }
    }
}
