package org.openstreetmap.atlas.generator.tools.spark.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.spark.converters.ConfigurationConverter;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.runtime.Command.Optionality;
import org.openstreetmap.atlas.utilities.runtime.Command.Switch;

/**
 * @author matthieun
 */
public final class PersistenceTools
{
    public static final String SHARDING_FILE = "sharding.txt";
    public static final String BOUNDARIES_FILE = "boundaries.txt.gz";
    public static final String SHARDING_META = "sharding.meta";
    public static final String BOUNDARIES_META = "boundaries.meta";

    public static final Switch<Boolean> COPY_SHARDING_AND_BOUNDARIES = new Switch<>(
            "copyShardingAndBoundaries",
            "Copy the sharding tree and boundary file used in this job, if any",
            Boolean::parseBoolean, Optionality.OPTIONAL, "false");

    private static final Integer BUFFER_SIZE = 4 * 1024;

    public static CountryBoundaryMap boundaries(final String input,
            final Map<String, String> configuration)
    {
        final Configuration hadoopConfiguration = ConfigurationConverter
                .mapToHadoopConfiguration(configuration);
        final Path inputPath = new Path(Paths.get(input, BOUNDARIES_FILE).toString());
        return CountryBoundaryMap.fromPlainText(new InputStreamResource(() ->
        {
            try
            {
                return inputPath.getFileSystem(hadoopConfiguration).open(inputPath);
            }
            catch (final IOException e)
            {
                throw new CoreException("Unable to open {}", inputPath.toUri().toString(), e);
            }
        }));
    }

    public static void copyShardingAndBoundariesToOutput(final String input, final String output,
            final Map<String, String> configuration)
    {
        final Configuration hadoopConfiguration = ConfigurationConverter
                .mapToHadoopConfiguration(configuration);
        copyToOutput(input, output, SHARDING_FILE, hadoopConfiguration);
        copyToOutput(input, output, SHARDING_META, hadoopConfiguration);
        copyToOutput(input, output, BOUNDARIES_FILE, hadoopConfiguration);
        copyToOutput(input, output, BOUNDARIES_META, hadoopConfiguration);
    }

    public static Sharding sharding(final String input, final Map<String, String> configuration)
    {
        final Configuration hadoopConfiguration = ConfigurationConverter
                .mapToHadoopConfiguration(configuration);
        final Path inputPath = new Path(Paths.get(input, SHARDING_FILE).toString());
        return AtlasSharding.forString("dynamic@" + inputPath.toUri().toString(),
                hadoopConfiguration);
    }

    private static void copyToOutput(final String input, final String output, final String name,
            final Configuration configuration)
    {
        final Path inputPath = new Path(Paths.get(input, name).toString());
        final Path outputPath = new Path(Paths.get(output, name).toString());
        try (InputStream inputStream = inputPath.getFileSystem(configuration).open(inputPath);
                OutputStream outputStream = outputPath.getFileSystem(configuration)
                        .create(outputPath))
        {
            IOUtils.copyBytes(inputStream, outputStream, BUFFER_SIZE, true);
        }
        catch (final IOException e)
        {
            throw new CoreException("Unable to copy {} to {}", input, output, e);
        }
    }

    private PersistenceTools()
    {
    }
}
