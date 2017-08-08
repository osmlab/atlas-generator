package org.openstreetmap.atlas.generator.tools.filesystem;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.converters.SparkConfToHadoopConfigurationConverter;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.utilities.collections.Maps;

/**
 * Utility class that helps create a {@link FileSystem} using some Spark/Hadoop configuration
 * key-values.
 *
 * @author matthieun
 */
public class FileSystemCreator
{
    private static final SparkConfToHadoopConfigurationConverter SPARK_CONF_TO_HADOOP_CONFIGURATION_CONVERTER = new SparkConfToHadoopConfigurationConverter();

    /**
     * @return The configuration needed to get a {@link ResourceFileSystem} setup.
     */
    public static final Map<String, String> resourceFileSystemScheme()
    {
        return Maps.hashMap(ResourceFileSystem.RESOURCE_FILE_SYSTEM_CONFIGURATION,
                ResourceFileSystem.class.getCanonicalName());
    }

    /**
     * Create a {@link FileSystem} from a Hadoop {@link Configuration}
     *
     * @param path
     *            The path that defines the {@link FileSystem}
     * @param configuration
     *            The {@link Configuration}
     * @return The corresponding {@link FileSystem}
     */
    public FileSystem get(final String path, final Configuration configuration)
    {
        try
        {
            return FileSystem.newInstance(new URI(path), configuration);
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not create FileSystem.", e);
        }
    }

    /**
     * Create a {@link FileSystem} from an agnostic configuration map
     *
     * @param path
     *            The path that defines the {@link FileSystem}
     * @param configuration
     *            The agnostic configuration map
     * @return The corresponding {@link FileSystem}
     */
    public FileSystem get(final String path, final Map<String, String> configuration)
    {
        final Configuration conf = new Configuration();
        for (final String key : configuration.keySet())
        {
            conf.set(key, configuration.get(key));
        }
        return get(path, conf);
    }

    /**
     * Create a {@link FileSystem} from a {@link SparkConf}
     *
     * @param path
     *            The path that defines the {@link FileSystem}
     * @param configuration
     *            The {@link SparkConf}
     * @return The corresponding {@link FileSystem}
     */
    public FileSystem get(final String path, final SparkConf configuration)
    {
        return get(path, SPARK_CONF_TO_HADOOP_CONFIGURATION_CONVERTER.convert(configuration));
    }
}
