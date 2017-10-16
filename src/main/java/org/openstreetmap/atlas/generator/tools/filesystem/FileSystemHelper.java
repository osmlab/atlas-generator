package org.openstreetmap.atlas.generator.tools.filesystem;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.streaming.resource.HDFSWalker;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.compression.Decompressor;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.OutputStreamWritableResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.WritableResource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that helps generating {@link Resource} and {@link WritableResource} from a Hadoop
 * Path.
 *
 * @author matthieun
 * @author tony
 */
public final class FileSystemHelper
{
    private static final Logger logger = LoggerFactory.getLogger(FileSystemHelper.class);
    private static final Map<String, String> DEFAULT = Maps.hashMap("fs.file.impl",
            RawLocalFileSystem.class.getCanonicalName());

    /**
     * Deletes given path using given configuration settings.
     *
     * @param path
     *            Path to delete
     * @param recursive
     *            If given path is a directory and this is set, then directory and all child items
     *            will be deleted, otherwise throws Exception.
     * @param configuration
     *            Configuration settings to use as context
     * @return true if deletion succeeded
     */
    public static boolean delete(final String path, final boolean recursive,
            final Map<String, String> configuration)
    {
        try
        {
            final FileSystem fileSystem = new FileSystemCreator().get(path, configuration);
            return fileSystem.delete(new Path(path), recursive);
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to delete {}", path, e);
        }
    }

    /**
     * @param directory
     *            The directory from which to recursively load files
     * @param configuration
     *            The configuration (containing the filesystem definition)
     * @param filter
     *            The path filter. If null, all the files will be returned.
     * @return a list of {@link Resource}s
     */
    public static List<Resource> listResourcesRecursively(final String directory,
            final Map<String, String> configuration, final PathFilter filter)
    {
        final List<Resource> resources = new ArrayList<>();
        final FileSystem fileSystem = new FileSystemCreator().get(directory, configuration);

        HDFSWalker.walk(new Path(directory)).map(HDFSWalker.debug(path -> logger.info("{}", path)))
                .forEach(status ->
                {
                    final Path path = status.getPath();
                    if (filter == null || filter.accept(path))
                    {
                        try
                        {
                            final InputStreamResource resource = new InputStreamResource(() ->
                            {
                                try
                                {
                                    return fileSystem.open(path);
                                }
                                catch (final Exception e)
                                {
                                    throw new CoreException("Unable to open {}", path, e);
                                }
                            }).withName(path.getName());

                            if (path.getName().endsWith(FileSuffix.GZIP.toString()))
                            {
                                resource.setDecompressor(Decompressor.GZIP);
                            }

                            resources.add(resource);
                        }
                        catch (final Exception e)
                        {
                            throw new CoreException("Unable to read {}", path, e);
                        }
                    }
                });

        return resources;
    }

    /**
     * Creates a new directory for given path using given configuration settings.
     *
     * @param path
     *            Path to use for directory creation operation
     * @param configuration
     *            Configuration settings to use as context
     * @return true if create operation succeeded
     */
    public static boolean mkdir(final String path, final Map<String, String> configuration)
    {
        try
        {
            final FileSystem fileSystem = new FileSystemCreator().get(path, configuration);
            return fileSystem.mkdirs(new Path(path));
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to mkdir {}", path, e);
        }
    }

    /**
     * Renames a source path to a destination path using the given configuration settings. This
     * assumes that the source and destination path file systems are the same. Hence, the source
     * path is used as a reference while initializing file system.
     *
     * @param sourcePath
     *            Path to rename from
     * @param destinationPath
     *            Path to rename to
     * @param configuration
     *            Configuration settings to use as context
     * @return true if rename operation succeeded
     */
    public static boolean rename(final String sourcePath, final String destinationPath,
            final Map<String, String> configuration)
    {
        try
        {
            final FileSystem fileSystem = new FileSystemCreator().get(sourcePath, configuration);
            return fileSystem.rename(new Path(sourcePath), new Path(destinationPath));
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to rename {} to {}", sourcePath, destinationPath, e);
        }
    }

    /**
     * @param path
     *            The path to create the resource from
     * @return A {@link Resource} coming from the default {@link RawLocalFileSystem}
     */
    public static Resource resource(final String path)
    {
        return resource(path, DEFAULT);
    }

    /**
     * @param path
     *            The path to create the resource from
     * @param configuration
     *            The configuration defining the {@link FileSystem}
     * @return A {@link Resource} coming from the appropriate {@link FileSystem}
     */
    public static Resource resource(final String path, final Map<String, String> configuration)
    {
        final FileSystem fileSystem = new FileSystemCreator().get(path, configuration);
        final Path hadoopPath = new Path(path);
        try
        {
            final InputStreamResource resource = new InputStreamResource(() ->
            {
                try
                {
                    return fileSystem.open(hadoopPath);
                }
                catch (final Exception e)
                {
                    throw new CoreException("Unable to open {}", hadoopPath, e);
                }
            }).withName(hadoopPath.getName());

            if (hadoopPath.getName().endsWith(FileSuffix.GZIP.toString()))
            {
                resource.setDecompressor(Decompressor.GZIP);
            }

            return resource;
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to read {}", hadoopPath, e);
        }
    }

    /**
     * List resources, but omit the hadoop "_SUCCESS" file.
     *
     * @param directory
     *            The directory from which to load files
     * @param configuration
     *            The configuration defining the {@link FileSystem}
     * @return a list of {@link Resource}s which contains all the files but the _SUCCESS file
     */
    public static List<Resource> resources(final String directory,
            final Map<String, String> configuration)
    {
        return resources(directory, configuration, path -> !path.getName().endsWith("_SUCCESS"));
    }

    /**
     * @param directory
     *            The directory from which to load files
     * @param configuration
     *            The configuration defining the {@link FileSystem}
     * @param filter
     *            The path filter. If null, all the files will be returned.
     * @return a list of {@link Resource}s
     */
    public static List<Resource> resources(final String directory,
            final Map<String, String> configuration, final PathFilter filter)
    {
        final List<Resource> resources = new ArrayList<>();
        final FileSystem fileSystem = new FileSystemCreator().get(directory, configuration);

        final FileStatus[] fileStatusList;
        try
        {
            fileStatusList = filter == null ? fileSystem.listStatus(new Path(directory))
                    : fileSystem.listStatus(new Path(directory), filter);
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not locate files on directory {}", directory, e);
        }

        for (final FileStatus fileStatus : fileStatusList)
        {
            final Path path = fileStatus.getPath();
            try
            {
                final InputStreamResource resource = new InputStreamResource(() ->
                {
                    try
                    {
                        return fileSystem.open(path);
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException("Unable to open {}", path, e);
                    }
                }).withName(path.getName());

                if (path.getName().endsWith(FileSuffix.GZIP.toString()))
                {
                    resource.setDecompressor(Decompressor.GZIP);
                }

                resources.add(resource);
            }
            catch (final Exception e)
            {
                throw new CoreException("Unable to read {}", path, e);
            }
        }

        return resources;
    }

    /**
     * @param path
     *            The path to create the resource from
     * @return A {@link WritableResource} coming from the default {@link RawLocalFileSystem}
     */
    public static WritableResource writableResource(final String path)
    {
        return writableResource(path, DEFAULT);
    }

    /**
     * @param path
     *            The path to create the resource from
     * @param configuration
     *            The configuration defining the {@link FileSystem}
     * @return A {@link WritableResource} coming from the appropriate {@link FileSystem}
     */
    public static WritableResource writableResource(final String path,
            final Map<String, String> configuration)
    {
        final Path hadoopPath = new Path(path);
        try
        {
            final FileSystem fileSystem = new FileSystemCreator().get(path, configuration);
            final OutputStream out;
            try
            {
                out = fileSystem.create(hadoopPath);
            }
            catch (final Exception e)
            {
                throw new CoreException("Unable to open {}", hadoopPath, e);
            }

            final OutputStreamWritableResource resource = new OutputStreamWritableResource(out);
            resource.setName(hadoopPath.getName());
            if (resource.isGzipped())
            {
                resource.setCompressor(Compressor.GZIP);
            }

            return resource;
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to read {}", hadoopPath, e);
        }
    }

    private FileSystemHelper()
    {
    }
}
