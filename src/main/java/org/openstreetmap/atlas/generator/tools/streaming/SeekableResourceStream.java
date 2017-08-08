package org.openstreetmap.atlas.generator.tools.streaming;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.streaming.Streams;
import org.openstreetmap.atlas.streaming.resource.Resource;

/**
 * Fake a {@link Seekable} stream from a {@link Resource} for testing
 *
 * @author matthieun
 */
public class SeekableResourceStream extends InputStream implements Seekable, PositionedReadable
{
    private final Resource resource;
    private final InputStream input;
    private final long pos;

    public SeekableResourceStream(final Resource resource)
    {
        this.resource = resource;
        this.input = resource.read();
        this.pos = 0;
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        catch (final IOException e)
        {
            throw new CoreException("Unable to close stream", e);
        }
        finally
        {
            Streams.close(this.input);
        }
    }

    @Override
    public long getPos() throws IOException
    {
        return this.pos;
    }

    @Override
    public int read() throws IOException
    {
        return this.input.read();
    }

    @Override
    public int read(final long position, final byte[] buffer, final int offset, final int length)
            throws IOException
    {
        return this.input.read(buffer, offset, length);
    }

    @Override
    public void readFully(final long position, final byte[] buffer) throws IOException
    {
        this.input.read(buffer, 0, (int) this.resource.length());
    }

    @Override
    public void readFully(final long position, final byte[] buffer, final int offset,
            final int length) throws IOException
    {
        this.input.read(buffer, offset, length);
    }

    @Override
    public void seek(final long pos) throws IOException
    {
    }

    @Override
    public boolean seekToNewSource(final long targetPos) throws IOException
    {
        return true;
    }
}
