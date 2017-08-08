package org.openstreetmap.atlas.generator.tools.persistence;

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
            private final boolean isCompressed = getCompressOutput(job);
            private final String resourceName = name + "." + fileExtension()
                    + (this.isCompressed ? ".gz" : "");
            private final Path path = FileOutputFormat.getTaskOutputPath(job, this.resourceName);
            private final FileSystem fileSystem = this.path.getFileSystem(job);
            private FSDataOutputStream dataOutputStream;

            @Override
            public void close(final Reporter reporter) throws IOException
            {
                Streams.close(this.dataOutputStream);
            }

            @Override
            public void write(final String key, final T value) throws IOException
            {
                // In case the stream gets corrupted somehow, re-try the upload.
                retry().run(() ->
                {
                    try
                    {
                        this.dataOutputStream = this.fileSystem.create(this.path, progress);
                        final AbstractWritableResource out = new OutputStreamWritableResource(
                                this.dataOutputStream);
                        out.setName(this.path.toString());
                        if (this.isCompressed)
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

    protected Retry retry()
    {
        return new Retry(RETRY_NUMBER, Duration.seconds(2));
    }

    protected abstract void save(T value, AbstractWritableResource out);
}
