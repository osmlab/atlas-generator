package org.openstreetmap.atlas.generator.tools.streaming.resource;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;

import com.google.common.collect.AbstractIterator;

/**
 * Inspired by Files.walk
 * (https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#walk-java.nio.file.Path-int-
 * java.nio.file.FileVisitOption...-), either retrieve a Stream of HDFS file status objects or
 * convert them to HDFSFile objects.
 *
 * @author cstaylor
 */
public final class HDFSWalker
{
    /**
     * Iterator for BFS over a collection of HDFS files and directories
     *
     * @author cstaylor
     */
    private static final class HDFSIterator extends AbstractIterator<FileStatus>
    {
        private final Queue<FileStatus> currentPaths;

        private final FileSystem fileSystem;

        private HDFSIterator(final Path root)
        {
            if (root == null)
            {
                throw new CoreException("Error when creating an HDFSIterator: root can't be null");
            }

            try
            {
                this.currentPaths = new LinkedList<>();
                this.fileSystem = root.getFileSystem(new Configuration());
                Stream.of(this.fileSystem.listStatus(root))
                        .forEach(status -> this.currentPaths.add(status));
            }
            catch (final IOException oops)
            {
                throw new CoreException("Error when creating an HDFSIterator", oops);
            }
        }

        @Override
        protected FileStatus computeNext()
        {
            if (this.currentPaths.isEmpty())
            {
                return endOfData();
            }

            final FileStatus returnValue = this.currentPaths.remove();
            if (returnValue.isDirectory())
            {
                try
                {
                    Stream.of(this.fileSystem.listStatus(returnValue.getPath()))
                            .forEach(status -> this.currentPaths.add(status));
                }
                catch (final IOException oops)
                {
                    throw new CoreException("Can't locate children of {}", returnValue, oops);
                }
            }
            return returnValue;
        }

    }

    public static HDFSFile convert(final FileStatus status)
    {
        try
        {
            return new HDFSFile(status.getPath());
        }
        catch (final IOException oops)
        {
            throw new CoreException("Error when converting FileStatus to HDFSFile", oops);
        }
    }

    public static Function<FileStatus, FileStatus> debug(final Consumer<String> printer)
    {
        return status ->
        {
            final char type = status.isSymlink() ? 'S' : status.isDirectory() ? 'D' : 'F';
            printer.accept(String.format("[%c] %s", type, status.getPath()));
            return status;
        };
    }

    public static Function<FileStatus, FileStatus> size(final AtomicLong value)
    {
        return status ->
        {
            value.addAndGet(status.getLen());
            return status;
        };
    }

    public static Stream<FileStatus> walk(final Path root)
    {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(new HDFSIterator(root), Spliterator.ORDERED),
                false);
    }

    public static Stream<HDFSFile> walkFiles(final Path root)
    {
        return walk(root).filter(FileStatus::isFile).map(HDFSWalker::convert);
    }

    private HDFSWalker()
    {
    }
}
