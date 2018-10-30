package org.openstreetmap.atlas.generator.tools.streaming;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
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
    private static final Map<String, Resource> STORE = new HashMap<>();
    public static final String SCHEME = "resource";
    public static final String RESOURCE_FILE_SYSTEM_CONFIGURATION = "fs." + SCHEME + ".impl";
    private static final Statistics STATISTICS = new Statistics(SCHEME);
    private URI uri;
    private Path workingDirectory;

    public static void addResource(final String name, final Resource resource)
    {
        STORE.put(name, resource);
    }

    public static SparkConf configuredConf()
    {
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

    public static Set<String> files()
    {
        return STORE.keySet();
    }

    public static void printContents()
    {
        files().forEach(file -> logger.info("{}", file));
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
        return new FSDataOutputStream(resource.write(), STATISTICS);
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
            return null;
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
