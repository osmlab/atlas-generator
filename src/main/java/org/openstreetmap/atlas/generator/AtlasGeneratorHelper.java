package org.openstreetmap.atlas.generator;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for {@link AtlasGenerator}.
 *
 * @author matthieun
 * @author mgostintsev
 */
public final class AtlasGeneratorHelper implements Serializable
{
    private static final long serialVersionUID = 1300098384789754747L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasGeneratorHelper.class);

    private static final String GZIPPED_ATLAS_EXTENSION = FileSuffix.ATLAS.toString()
            + FileSuffix.GZIP.toString();
    private static final String ATLAS_EXTENSION = FileSuffix.ATLAS.toString();
    private static final AtlasResourceLoader ATLAS_LOADER = new AtlasResourceLoader();

    /**
     * @param atlasDirectory
     *            The path of the folder containing the Atlas files, in format CTRY_z-x-y.atlas.gz
     * @param temporaryDirectory
     *            The path of the temporary folder to download Atlas files if they are not
     *            downloaded already
     * @param country
     *            The country to look for
     * @param sparkContext
     *            The Spark configuration as a map (to allow the creation of the proper FileSystem)
     * @param validShards
     *            All available shards for given country, to avoid fetching shards that do not exist
     * @return A function that returns an {@link Atlas} given a {@link Shard}
     */
    public static Function<Shard, Optional<Atlas>> atlasFetcher(final String atlasDirectory,
            final String temporaryDirectory, final String country,
            final Map<String, String> sparkContext, final Set<Shard> validShards)
    {
        // & Serializable is very important as that function will be passed around by Spark, and
        // functions are not serializable by default.
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            if (!validShards.contains(shard))
            {
                return Optional.empty();
            }

            // Check if non-gzipped file exists in final temporary directory
            final String pathFromTemporaryDirectory = SparkFileHelper.combine(temporaryDirectory,
                    String.format("%s%s", getAtlasName(country, shard), ATLAS_EXTENSION));
            final File fileFromTemporaryDirectory = new File(pathFromTemporaryDirectory);

            // Download file to disk if it is not cached already
            if (!fileFromTemporaryDirectory.exists())
            {
                try
                {
                    String path = SparkFileHelper.combine(atlasDirectory,
                            String.format("%s%s", getAtlasName(country, shard), ATLAS_EXTENSION));

                    if (!fileExists(path, sparkContext))
                    {
                        path = SparkFileHelper.combine(atlasDirectory, String.format("%s%s",
                                getAtlasName(country, shard), GZIPPED_ATLAS_EXTENSION));
                    }

                    final Resource fileFromNetwork = FileSystemHelper.resource(path, sparkContext);
                    final File temporaryLocalFile = File
                            .temporary(getAtlasName(country, shard) + "-", ATLAS_EXTENSION);

                    System.out.println("Downloaded atlas from " + path
                            + " and is found as temp file " + temporaryLocalFile.getAbsolutePath());

                    // FileSystemHelper.resource sets the Decompressor on the Resource for us, so
                    // this call will gunzip the file
                    fileFromNetwork.copyTo(temporaryLocalFile);

                    // Before making the move, check again if file is there or not
                    if (!fileFromTemporaryDirectory.exists())
                    {
                        try
                        {
                            Files.move(Paths.get(temporaryLocalFile.getPath()),
                                    Paths.get(fileFromTemporaryDirectory.getPath()),
                                    StandardCopyOption.ATOMIC_MOVE);
                        }
                        catch (final FileAlreadyExistsException e)
                        {
                            logger.warn("Failed to rename file, but file exists already.", e);
                        }
                        catch (final Exception e)
                        {
                            logger.warn("Failed to rename file on local disk.", e);
                        }
                    }
                }
                catch (final Exception e)
                {
                    logger.warn("Failed to cache file on local disk.", e);
                }
            }

            // If we were able to find the file on local disk, then load from there
            if (fileFromTemporaryDirectory.exists())
            {
                System.out.println("AtlasExisted - Cache Hit: "
                        + fileFromTemporaryDirectory.getAbsolutePath());
                return loadAtlas(fileFromTemporaryDirectory);
            }
            else
            {
                logger.warn("Falling back to Atlas file hosted on {} for shard {}.", atlasDirectory,
                        shard.getName());
                final String path = SparkFileHelper.combine(atlasDirectory,
                        String.format("%s%s", getAtlasName(country, shard), ATLAS_EXTENSION));
                final Resource fileFromNetwork = FileSystemHelper.resource(path, sparkContext);
                return loadAtlas(fileFromNetwork);
            }
        };
    }

    private static boolean fileExists(final String path, final Map<String, String> configuration)
    {
        final FileSystem fileSystem = new FileSystemCreator().get(path, configuration);
        try
        {
            return fileSystem.exists(new Path(path));
        }
        catch (IllegalArgumentException | IOException e)
        {
            logger.warn("can't determine if {} exists", path);
            return false;
        }
    }

    private static String getAtlasName(final String country, final Shard shard)
    {
        return String.format("%s_%s", country, shard.getName());
    }

    private static Optional<Atlas> loadAtlas(final Resource file)
    {
        return Optional.ofNullable(ATLAS_LOADER.load(file));
    }

    /**
     * Hide constructor for this utility class.
     */
    private AtlasGeneratorHelper()
    {
    }
}
