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
 * java.nio.file.FileVisitOption...-), either retrieve a Stream of HDFS file
 * status objects or convert them to HDFSFile objects.
 *
 * @author cstaylor
 * @author jklamer
 * @author sbhalekar
 */
public final class HDFSWalker {
	/**
	 * Collection of hadoop file status object and it's corresponding depth in
	 * the filesystem hierarchy
	 *
	 * @author sbhalekar
	 */
	private static final class FileStatusAtDepth {
		private final FileStatus fileStatus;
		private final int fileDepth;

		private FileStatusAtDepth(final FileStatus fileStatus, final int fileLevel) {
			this.fileStatus = fileStatus;
			this.fileDepth = fileLevel;
		}

		public int getFileDepth() {
			return this.fileDepth;
		}

		public FileStatus getFileStatus() {
			return this.fileStatus;
		}
	}

	/**
	 * Iterator for BFS over a collection of HDFS files and directories
	 *
	 * @author cstaylor
	 */
	private static final class HDFSIterator extends AbstractIterator<FileStatus> {

		/**
		 * This variable represents the depth for all the children of root
		 * directory
		 */
		private static final int ONE = 1;

		private final Queue<FileStatusAtDepth> currentPaths;

		private final FileSystem fileSystem;

		/**
		 * Limit to stop traversing the hadoop directory hierarchy. All the file
		 * paths would be explored if this limit exceeds the max depth in
		 * filesystem hierarchy. Root will always have a depth of 0.
		 */
		private final int maxDepth;

		private HDFSIterator(final Path root, final Configuration configuration) {
			this(root, configuration, HDFSWalker.WALK_ALL);
		}

		private HDFSIterator(final Path root, final Configuration configuration, int maxDepth) {
			if (root == null) {
				throw new CoreException("Error when creating an HDFSIterator: root can't be null");
			}
			this.maxDepth = maxDepth;

			try {
				this.currentPaths = new LinkedList<>();
				this.fileSystem = root.getFileSystem(configuration);
				// Files at depth 1 will be traversed all the time
				Stream.of(this.fileSystem.listStatus(root))
						.forEach(status -> this.currentPaths.add(new FileStatusAtDepth(status, ONE)));
			} catch (final IOException oops) {
				throw new CoreException("Error when creating an HDFSIterator", oops);
			}
		}

		@Override
		protected FileStatus computeNext() {
			if (this.currentPaths.isEmpty()) {
				return endOfData();
			}

			final FileStatusAtDepth returnValue = this.currentPaths.remove();
			final FileStatus currentFileStatus = returnValue.getFileStatus();
			final int currentFileDepth = returnValue.getFileDepth();

			if (currentFileStatus.isDirectory()) {
				int childDepth = currentFileDepth + ONE;
				// Add the children only if they are below the threshold depth
				// or the threshold is not specified at all
				if (this.maxDepth == HDFSWalker.WALK_ALL || childDepth <= this.maxDepth)
					try {
						Stream.of(this.fileSystem.listStatus(currentFileStatus.getPath()))
								.forEach(status -> this.currentPaths.add(new FileStatusAtDepth(status, childDepth)));
					} catch (final IOException oops) {
						throw new CoreException("Can't locate children of {}", currentFileStatus, oops);
					}
			}
			return currentFileStatus;
		}
	}

	private Configuration configuration;
	private final int maxDepth;
	public static final int WALK_ALL = -1;

	public HDFSWalker() {
		this.maxDepth = WALK_ALL;
	}

	public HDFSWalker(int maxDepth) {
		this.maxDepth = maxDepth;
	}

	public static HDFSFile convert(final FileStatus status) {
		try {
			return new HDFSFile(status.getPath());
		} catch (final IOException oops) {
			throw new CoreException("Error when converting FileStatus to HDFSFile", oops);
		}
	}

	public static Function<FileStatus, FileStatus> debug(final Consumer<String> printer) {
		return status -> {
			final char type = status.isSymlink() ? 'S' : status.isDirectory() ? 'D' : 'F';
			printer.accept(String.format("[%c] %s", type, status.getPath()));
			return status;
		};
	}

	public static Function<FileStatus, FileStatus> size(final AtomicLong value) {
		return status -> {
			value.addAndGet(status.getLen());
			return status;
		};
	}

	public HDFSWalker usingConfiguration(final Configuration configuration) {
		this.configuration = configuration;
		return this;
	}

	public Stream<FileStatus> walk(final Path root) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
				new HDFSIterator(root, getConfiguration(), this.maxDepth), Spliterator.ORDERED), false);
	}

	public Stream<HDFSFile> walkFiles(final Path root) {
		return walk(root).filter(FileStatus::isFile).map(HDFSWalker::convert);
	}

	private Configuration getConfiguration() {
		return this.configuration == null ? new Configuration() : this.configuration;
	}
}
