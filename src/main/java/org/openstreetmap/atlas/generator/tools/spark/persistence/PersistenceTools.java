package org.openstreetmap.atlas.generator.tools.spark.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
public class PersistenceTools
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
    private final Map<String, String> configurationMap;

    public PersistenceTools(final Map<String, String> configurationMap)
    {
        this.configurationMap = configurationMap;
    }

    public CountryBoundaryMap boundaries(final String input)
    {
        final Configuration hadoopConfiguration = hadoopConfiguration();
        final Path inputPath = new Path(appendDirectorySeparator(input) + BOUNDARIES_FILE);
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

    public void copyShardingAndBoundariesToOutput(final String input, final String output)
    {
        copyToOutput(input, output, SHARDING_FILE);
        copyToOutput(input, output, SHARDING_META);
        copyToOutput(input, output, BOUNDARIES_FILE);
        copyToOutput(input, output, BOUNDARIES_META);
    }

    public Sharding sharding(final String input)
    {
        final Configuration hadoopConfiguration = hadoopConfiguration();
        final Path inputPath = new Path(appendDirectorySeparator(input) + SHARDING_FILE);
        return AtlasSharding.forString("dynamic@" + inputPath.toUri().toString(),
                hadoopConfiguration);
    }

    private String appendDirectorySeparator(final String input)
    {
        final String inputString;
        if (input.endsWith("/"))
        {
            inputString = input;
        }
        else
        {
            inputString = input + "/";
        }
        return inputString;
    }

    private void copyToOutput(final String input, final String output, final String name)
    {
        final Path inputPath = new Path(appendDirectorySeparator(input) + name);
        final Path outputPath = new Path(appendDirectorySeparator(output) + name);
        final Configuration configuration = hadoopConfiguration();
        try (InputStream inputStream = inputPath.getFileSystem(configuration).open(inputPath);
                OutputStream outputStream = outputPath.getFileSystem(configuration)
                        .create(outputPath))
        {
            if (inputStream == null)
            {
                throw new CoreException(
                        "{} does not exist and thus cannot be copied to the output.", inputPath);
            }
            IOUtils.copyBytes(inputStream, outputStream, BUFFER_SIZE, true);
        }
        catch (final IOException e)
        {
            throw new CoreException("Unable to copy {} to {}", input, output, e);
        }
    }

    private Configuration hadoopConfiguration()
    {
        return ConfigurationConverter.mapToHadoopConfiguration(this.configurationMap);
    }
}
