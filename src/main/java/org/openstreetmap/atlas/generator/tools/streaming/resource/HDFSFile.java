package org.openstreetmap.atlas.generator.tools.streaming.resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.compression.Decompressor;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.utilities.runtime.Retry;
import org.openstreetmap.atlas.utilities.scalars.Duration;

/**
 * Readable and Writable resource for HDFS files
 *
 * @author cuthbertm
 */
public class HDFSFile extends AbstractWritableResource
{
    private static final int DEFAULT_RETRIES = 3;
    private static final Duration DEFAULT_RETRYWAIT = Duration.seconds(2);

    private final Path path;
    private final FileSystem system;
    private boolean appendToFile = false;
    private int retries = DEFAULT_RETRIES;
    private Duration retryWait = DEFAULT_RETRYWAIT;

    public HDFSFile(final Path path) throws IOException
    {
        this.path = path;
        this.system = FileSystem.get(path.toUri(), new Configuration());
        FileSuffix.suffixFor(path.getName()).ifPresent(suffix ->
        {
            if (suffix == FileSuffix.GZIP)
            {
                setCompressor(Compressor.GZIP);
                setDecompressor(Decompressor.GZIP);
            }
        });
    }

    public HDFSFile(final String path) throws IOException
    {
        this(new Path(path));
    }

    public HDFSFile(final String server, final int port, final String path)
            throws URISyntaxException, IOException
    {
        this(new Path(String.format("hdfs://%s:%d/%s", server, port, path)));
    }

    public boolean exists()
    {
        try
        {
            return this.system.exists(this.path);
        }
        catch (final IOException oops)
        {
            throw new CoreException("Could not check if path {} exists", this.path.toString(),
                    oops);
        }
    }

    public Path getPath()
    {
        return this.path;
    }

    public int getRetries()
    {
        return this.retries;
    }

    public Duration getRetryWait()
    {
        return this.retryWait;
    }

    public boolean isAppendToFile()
    {
        return this.appendToFile;
    }

    public boolean isDirectory()
    {
        try
        {
            return this.system.isDirectory(this.path);
        }
        catch (final IOException e)
        {
            throw new CoreException("Could not check if path {} is a directory",
                    this.path.toString(), e);
        }
    }

    @Override
    public long length()
    {
        try
        {
            return this.system.getFileStatus(this.path).getLen();
        }
        catch (final IOException e)
        {
            throw new CoreException("Could not check length of path {}", this.path.toString(), e);
        }
    }

    public void mkdirs(final boolean clean)
    {
        new Retry(this.retries, this.retryWait).run(() ->
        {
            try
            {
                if (clean)
                {
                    this.system.delete(this.path, true);
                }
                this.system.mkdirs(this.path);
            }
            catch (final IOException oops)
            {
                throw new CoreException("Could not create directories in HDFS for location {}",
                        this.path, oops);
            }
        });
    }

    /**
     * CAUTION: Please use this wisely in case of removing directories recursively
     *
     * @param recursive
     *            true to recursively delete a directory
     */
    public void remove(final boolean recursive)
    {
        new Retry(this.retries, this.retryWait).run(() ->
        {
            try
            {
                this.system.delete(this.path, recursive);
            }
            catch (final IOException oops)
            {
                throw new CoreException("Could not delete path in HDFS location {}", this.path,
                        oops);
            }
        });
    }

    public void setAppendToFile(final boolean appendToFile)
    {
        this.appendToFile = appendToFile;
    }

    public void setRetries(final int retries)
    {
        this.retries = retries;
    }

    public void setRetryWait(final Duration retryWait)
    {
        this.retryWait = retryWait;
    }

    @Override
    protected InputStream onRead()
    {
        return new Retry(this.retries, this.retryWait).run(() ->
        {
            try
            {
                return this.system.open(this.path);
            }
            catch (final IOException ioe)
            {
                throw new CoreException("Could not read file.", ioe);
            }
        });
    }

    @Override
    protected OutputStream onWrite()
    {
        return new Retry(this.retries, this.retryWait).run(() ->
        {
            try
            {
                if (this.system.exists(this.path))
                {
                    if (this.appendToFile)
                    {
                        return this.system.append(this.path);
                    }
                    else
                    {
                        this.system.delete(this.path, true);
                    }
                }

                return this.system.create(this.path);
            }
            catch (final IOException ioe)
            {
                throw new CoreException("Could not write to file.", ioe);
            }
        });
    }
}
