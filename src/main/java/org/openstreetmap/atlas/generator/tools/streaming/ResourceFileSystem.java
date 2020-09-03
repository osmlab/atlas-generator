package org.openstreetmap.atlas.generator.tools.streaming;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.SparkConf;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.streaming.resource.WritableResource;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileSystem} that is based on a set of resources.
 *
 * @author matthieun
 */
public class ResourceFileSystem extends FileSystem
{
    private static final Logger logger = LoggerFactory.getLogger(ResourceFileSystem.class);

    // The store that contains all the known resources in the file system
    private static final Map<String, Resource> STORE = new ConcurrentHashMap<>();
    public static final String SCHEME = "resource";
    public static final String RESOURCE_FILE_SYSTEM_CONFIGURATION = "fs." + SCHEME + ".impl";
    private static final Statistics STATISTICS_INTERNAL = new Statistics(SCHEME);
    private static Class<?> clazz = null;

    private URI uri;
    private Path workingDirectory;

    public static void addResource(final String path, final String name, final boolean gzipIt,
            final Class<?> clazz)
    {
        Resource input = new InputStreamResource(() -> clazz.getResourceAsStream(name));
        if (gzipIt)
        {
            final ByteArrayResource newInput = new ByteArrayResource();
            newInput.setCompressor(Compressor.GZIP);
            input.copyTo(newInput);
            input = newInput;
        }
        addResource(path, input);
    }

    public static void addResource(final String path, final String name, final boolean gzipIt)
    {
        if (clazz == null)
        {
            throw new CoreException("Need to register a class to find the resource!");
        }
        addResource(path, name, gzipIt, clazz);
    }

    public static void addResource(final String name, final Resource resource)
    {
        STORE.put(name, resource);
    }

    public static void addResource(final String path, final String name)
    {
        addResource(path, name, false);
    }

    public static void addResourceContents(final String path, final String contents)
    {
        addResource(path, new StringResource(contents));
    }

    public static void clear()
    {
        STORE.clear();
    }

    public static SparkConf configuredConf()
    {
        // @TODO: replace with inclusive language once
        // https://issues.apache.org/jira/browse/SPARK-32333 is completed
        final SparkConf result = new SparkConf();
        result.set(RESOURCE_FILE_SYSTEM_CONFIGURATION, ResourceFileSystem.class.getCanonicalName());
        result.set("spark.master", "local");
        result.set("spark.app.name", "appName");
        return result;
    }

    public static Configuration configuredConfiguration()
    {
        final Configuration result = new Configuration();
        result.set(RESOURCE_FILE_SYSTEM_CONFIGURATION, ResourceFileSystem.class.getCanonicalName());
        return result;
    }

    public static void dumpToDisk(final File folder)
    {
        files().forEach(file ->
        {
            final String subPath = file.substring(String.valueOf(SCHEME + "://").length());
            final File output = folder.child(subPath);
            output.withCompressor(Compressor.NONE);
            STORE.get(file).copyTo(output);
        });
    }

    public static Set<String> files()
    {
        return STORE.keySet();
    }

    public static Optional<PackedAtlas> getAtlas(final String path)
    {
        if (!path.endsWith(FileSuffix.ATLAS.toString()))
        {
            throw new CoreException("Cannot read resource {} as an Atlas.", path);
        }
        return getResource(path).map(PackedAtlas::load);
    }

    public static PackedAtlas getAtlasOrElse(final String path)
    {
        return getAtlas(path).orElseThrow(() -> new CoreException("{} not found.", path));
    }

    public static Optional<Resource> getResource(final String path)
    {
        if (!path.startsWith(SCHEME + "://"))
        {
            throw new CoreException("Cannot read resource {} in a {}", path,
                    ResourceFileSystem.class.getSimpleName());
        }
        return Optional.ofNullable(SparkJob.resource(path, simpleconfiguration()));
    }

    public static Resource getResourceOrElse(final String path)
    {
        return getResource(path).orElseThrow(() -> new CoreException("{} not found.", path));
    }

    public static void printContents()
    {
        if (logger.isInfoEnabled())
        {
            files().forEach(file -> logger.info("{} (length: {})", file,
                    getResource(file)
                            .orElseThrow(() -> new CoreException("{} could not be found.", file))
                            .length()));
        }
    }

    public static synchronized void registerResourceExtractionClass(final Class<?> clazz)
    {
        ResourceFileSystem.clazz = clazz;
    }

    public static Map<String, String> simpleconfiguration()
    {
        final Map<String, String> result = new HashMap<>();
        result.put(RESOURCE_FILE_SYSTEM_CONFIGURATION, ResourceFileSystem.class.getCanonicalName());
        return result;
    }

    public ResourceFileSystem()
    {
        setConf(configuredConfiguration());
    }

    @Override
    public FSDataOutputStream append(final Path hadoopPath, final int bufferSize,
            final Progressable progress) throws IOException
    {
        throw new CoreException("Not supported.");
    }

    @Override
    public FSDataOutputStream create(final Path hadoopPath, final FsPermission permission,
            final boolean overwrite, final int bufferSize, final short replication,
            final long blockSize, final Progressable progress) throws IOException
    {
        if (STORE.containsKey(hadoopPath.toString()))
        {
            delete(hadoopPath, false);
        }
        final String name = hadoopPath.toString();
        final WritableResource resource = new ByteArrayResource().withName(name);
        STORE.put(name, resource);
        return new FSDataOutputStream(resource.write(), STATISTICS_INTERNAL);
    }

    @Override
    public boolean delete(final Path hadoopPath, final boolean recursive) throws IOException
    {
        STORE.remove(hadoopPath.toString());
        return true;
    }

    @Override
    public FileStatus getFileStatus(final Path hadoopPath) throws IOException
    {
        final Resource resource = STORE.get(hadoopPath.toString());
        if (resource == null)
        {
            for (final String filePath : STORE.keySet())
            {
                if (filePath.startsWith(hadoopPath.toString()))
                {
                    return new FileStatus(0, true, 0, 0, 0, hadoopPath);
                }
            }
            throw new FileNotFoundException();
        }
        return new FileStatus(resource.length(), false, 1, Long.MAX_VALUE, 0, hadoopPath);
    }

    @Override
    public URI getUri()
    {
        return this.uri;
    }

    @Override
    public Path getWorkingDirectory()
    {
        return this.workingDirectory;
    }

    @Override
    public void initialize(final URI uri, final Configuration conf) throws IOException
    {
        try
        {
            setConf(conf);
            this.uri = uri;
            super.initialize(uri, conf);
        }
        catch (final Exception e)
        {
            throw new IOException(e);
        }
    }

    @Override
    public FileStatus[] listStatus(final Path hadoopPath) throws FileNotFoundException, IOException
    {
        final List<FileStatus> result = new ArrayList<>();
        final String prefix = hadoopPath.toString();
        for (final String filePath : STORE.keySet())
        {
            if (filePath.startsWith(prefix))
            {
                result.add(new FileStatus(0, false, 0, 0, 0, new Path(filePath)));
            }
        }
        return result.toArray(new FileStatus[0]);
    }

    @Override
    public boolean mkdirs(final Path hadoopPath, final FsPermission permission) throws IOException
    {
        return true;
    }

    @Override
    public FSDataInputStream open(final Path hadoopPath, final int bufferSize) throws IOException
    {
        final String name = hadoopPath.toString();
        final Resource resource = STORE.get(name);
        if (resource == null)
        {
            throw new FileNotFoundException("Path does not exist or is a directory: " + hadoopPath);
        }
        return new FSDataInputStream(new SeekableResourceStream(resource));
    }

    @Override
    public boolean rename(final Path source, final Path destination) throws IOException
    {
        final String sourceName = source.toString();
        String destinationName = destination.toString();
        if (sourceName.startsWith(destinationName))
        {
            // Hadoop end of job rename case
            destinationName = destinationName.endsWith("/") ? destinationName
                    : destinationName + "/";
            // Sample resource:
            // resource://test/atlas/_temporary/0/_temporary/attempt_201804061442_0003_m_000002_14/DMA/9/DMA_9-168-234.atlas
            final Pattern pattern = Pattern.compile("\\/attempt_[^\\/]*\\/");
            final Matcher matcher = pattern.matcher(sourceName);
            final String appendName;
            if (matcher.find())
            {
                final String match = matcher.group(0);
                final StringList split = StringList.split(sourceName, match);
                appendName = split.get(1);
            }
            else
            {
                appendName = sourceName.substring(sourceName.lastIndexOf("/") + 1);
            }
            destinationName = destinationName + appendName;
        }
        if (STORE.containsKey(sourceName))
        {
            if (STORE.containsKey(destinationName))
            {
                delete(new Path(destinationName), false);
            }
            final Resource resource = STORE.get(sourceName);
            STORE.put(destinationName, resource);
            STORE.remove(sourceName);
        }
        return true;
    }

    @Override
    public void setWorkingDirectory(final Path newDirectory)
    {
        this.workingDirectory = newDirectory;
    }
}
