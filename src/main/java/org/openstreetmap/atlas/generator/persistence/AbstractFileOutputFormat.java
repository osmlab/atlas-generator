package org.openstreetmap.atlas.generator.persistence;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.streaming.Streams;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.OutputStreamWritableResource;
import org.openstreetmap.atlas.utilities.runtime.Retry;
import org.openstreetmap.atlas.utilities.scalars.Duration;

/**
 * Abstract implementation of FileOutputFormat that takes care of the {@link RecordWriter}
 *
 * @author matthieun
 * @param <T>
 *            The type to save
 */
public abstract class AbstractFileOutputFormat<T> extends FileOutputFormat<String, T>
{
    private static final int RETRY_NUMBER = 5;

    @Override
    public RecordWriter<String, T> getRecordWriter(final FileSystem ignored, final JobConf job,
            final String name, final Progressable progress) throws IOException
    {
        return new RecordWriter<String, T>()
        {
            private final String resourceName = getQualifiedFileNameWithExtension(name);
            private final Path outputPath = FileOutputFormat.getOutputPath(job);
            private final Path path = new Path(this.outputPath, this.resourceName);
            private final FileSystem fileSystem = this.path.getFileSystem(job);
            private FSDataOutputStream dataOutputStream;
            private String lastKey;
            private T lastValue;

            @Override
            public void close(final Reporter reporter)
            {
                retry().run(() ->
                {
                    try
                    {
                        Streams.close(this.dataOutputStream);
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException("Unable to close stream for last key {}",
                                this.lastKey, e);
                    }
                }, () ->
                {
                    // Below is the "runBeforeRetry" which re-attempts the upload if a "close"
                    // above fails.
                    if (this.lastKey != null && this.lastValue != null)
                    {
                        write(this.lastKey, this.lastValue);
                    }
                });
                this.lastKey = null;
                this.lastValue = null;
            }

            @Override
            public void write(final String key, final T value)
            {
                this.lastKey = key;
                this.lastValue = value;
                // In case the stream gets corrupted somehow, re-try the upload.
                retry().run(() ->
                {
                    try
                    {
                        this.dataOutputStream = this.fileSystem.create(this.path, progress);
                        final AbstractWritableResource out = new OutputStreamWritableResource(
                                this.dataOutputStream);
                        out.setName(this.path.toString());
                        if (isCompressed())
                        {
                            out.setCompressor(Compressor.GZIP);
                        }
                        save(value, out);
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException("File Output Format: Failed to save {}",
                                this.path.toString(), e);
                    }
                });
            }
        };
    }

    protected abstract String fileExtension();

    protected abstract boolean isCompressed();

    protected Retry retry()
    {
        return new Retry(RETRY_NUMBER, Duration.seconds(2)).withQuadratic(true);
    }

    protected abstract void save(T value, AbstractWritableResource out);

    private String getQualifiedFileNameWithExtension(final String name)
    {
        if (this.isCompressed())
        {
            return String.format("%s%s%s", name, this.fileExtension(), FileSuffix.GZIP.toString());
        }
        else
        {
            return String.format("%s%s", name, this.fileExtension());
        }
    }
}
