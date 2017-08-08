package org.openstreetmap.atlas.generator.tools.spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.streaming.compression.Decompressor;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locate and retrieve resources from a data store.
 *
 * @param <T>
 *            The type of the data to locate
 * @author matthieun
 */
public abstract class DataLocator<T> implements Serializable
{
    /**
     * Locator that returns {@link Resource}s.
     *
     * @author matthieun
     */
    public static final class ResourceLocator extends DataLocator<Resource>
    {
        private static final long serialVersionUID = 3528162777067011094L;

        public ResourceLocator(final Map<String, String> sparkContext)
        {
            super(sparkContext);
        }

        @Override
        protected Optional<Resource> readFrom(final Resource resource)
        {
            return Optional.ofNullable(resource);
        }
    }

    private static final long serialVersionUID = 6569907149273805927L;

    private static final Logger logger = LoggerFactory.getLogger(DataLocator.class);

    private final Map<String, String> sparkContext;

    public DataLocator(final Map<String, String> sparkContext)
    {
        this.sparkContext = sparkContext;
    }

    /**
     * Retrieve some resources
     *
     * @param paths
     *            The paths to get
     * @return The resources
     */
    public Iterable<T> retrieve(final Iterable<String> paths)
    {
        return Iterables.stream(paths).map(this::retrieve).filter(option -> option.isPresent())
                .map(option -> option.get()).collect();
    }

    /**
     * Retrieve a resource
     *
     * @param path
     *            The path of the resource
     * @return The resource option
     */
    public Optional<T> retrieve(final String path)
    {
        final Path value = new Path(path);
        final FileSystem fileSystem = new FileSystemCreator().get(value.toUri().toString(),
                this.sparkContext);
        try
        {
            // Do not use FileSystem.exists() here as it does not play best with elastic file
            // systems.
            FileStatus fileStatus = null;
            try
            {
                fileStatus = fileSystem.getFileStatus(value);
            }
            catch (final FileNotFoundException e)
            {
                // File is not there
            }
            if (fileStatus == null || !fileStatus.isFile())
            {
                logger.warn("Resource {} does not exist.", value.toString());
                return Optional.empty();
            }
        }
        catch (final IOException e)
        {
            throw new CoreException("Cannot test if {} exists.", value.toString(), e);
        }
        final InputStreamResource resource = new InputStreamResource(() ->
        {
            try
            {
                return fileSystem.open(value);
            }
            catch (final Exception e)
            {
                throw new CoreException("Cannot translate {} to a resource.", value, e);
            }
        }).withName(path);
        if (path.endsWith(FileSuffix.GZIP.toString()))
        {
            resource.setDecompressor(Decompressor.GZIP);
        }
        return readFrom(resource);
    }

    /**
     * Transform a resource into a fetched object
     *
     * @param resource
     *            The resource to read
     * @return The fetched object
     */
    protected abstract Optional<T> readFrom(Resource resource);
}
