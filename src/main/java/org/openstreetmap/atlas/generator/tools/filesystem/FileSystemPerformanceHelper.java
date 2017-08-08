package org.openstreetmap.atlas.generator.tools.filesystem;

import java.util.concurrent.Callable;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.utilities.scalars.Duration;
import org.openstreetmap.atlas.utilities.threads.Pool;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When working with a FileSystem that is slow at renaming, this provides a hack to speed up the
 * rename of output files. The rename calls made by Spark are sent to many threads.
 *
 * @author matthieun
 */
public final class FileSystemPerformanceHelper
{
    private static final Logger logger = LoggerFactory.getLogger(FileSystemPerformanceHelper.class);

    public static final int NUMBER_POOL_THREADS = 50;
    public static final int POOL_MINUTES_BEFORE_KILL = 30;

    private static Pool RENAME_POOL = null;

    public static boolean executeRename(final Callable<Boolean> renameOperation)
    {
        if (RENAME_POOL == null)
        {
            // Do the usual if the rename pool has not been started.
            try
            {
                return renameOperation.call();
            }
            catch (final Exception e)
            {
                throw new CoreException("Unable to run rename operation", e);
            }
        }
        else
        {
            // If the rename pool has been started, use it to bulk the rename operations.
            RENAME_POOL.queue(renameOperation);
            return true;
        }
    }

    public static void openRenamePool()
    {
        logger.warn("Opening rename thread pool!");
        RENAME_POOL = new Pool(NUMBER_POOL_THREADS, "renamePool");
    }

    public static void waitForAndCloseRenamePool()
    {
        logger.warn("Closing rename thread pool!");
        final Time start = Time.now();
        final Duration maxDuration = Duration.minutes(POOL_MINUTES_BEFORE_KILL);
        RENAME_POOL.end(maxDuration);
        RENAME_POOL = null;
        logger.warn("Closed rename thread pool in {}!", start.elapsedSince());
    }

    private FileSystemPerformanceHelper()
    {
    }
}
