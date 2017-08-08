package org.openstreetmap.atlas.generator.tools.streaming.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.openstreetmap.atlas.streaming.NotifyingIOUtils;
import org.openstreetmap.atlas.streaming.NotifyingIOUtils.IOProgressListener;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StreamOfResourceStreams;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

/**
 * Sample program showing how to use the HDFSWalker utility class
 *
 * @author cstaylor
 */
public class HDFSCommand extends HDFSArgumentsCommand
{
    /**
     * Sample IOProgressListener implementation that outputs to the console
     *
     * @author cstaylor
     */
    private static final class MyIOProgressListener implements IOProgressListener
    {
        private long total = 0;

        private long current = 0;

        private MyIOProgressListener(final long total)
        {
            this.total = total;
        }

        @Override
        public void completed()
        {
            System.out.printf("Done\n");
        }

        @Override
        public void failed(final IOException oops)
        {
            oops.printStackTrace();
        }

        @Override
        public void started()
        {
            System.out.printf("Started\n");
        }

        @Override
        public void statusUpdate(final long count)
        {
            this.current = count;
            System.out.printf("Read: %d/%d\n", this.current, this.total);
        }
    }

    public static void main(final String... args)
    {
        new HDFSCommand().runWithoutQuitting(args);
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        super.onRun(command);
        HDFSWalker.walk(getRoot()).map(HDFSWalker.debug(System.out)).forEach(status ->
        {
            // Do something with the HDFS FileStatus
        });

        final AtomicLong iosize = new AtomicLong();
        try (InputStream stream = new StreamOfResourceStreams(HDFSWalker.walk(getRoot())
                .filter(FileStatus::isFile).map(HDFSWalker.size(iosize)).map(HDFSWalker::convert)
                .collect(Collectors.toList()).toArray(new Resource[0])))
        {
            // Try it with a listener
            NotifyingIOUtils.copy(stream, System.out, new MyIOProgressListener(iosize.get()));
            // Or without
            // NotifyingIOUtils.copy(stream, System.out, null);

        }
        catch (final IOException oops)
        {
            oops.printStackTrace();
            return 1;
        }
        return 0;
    }
}
