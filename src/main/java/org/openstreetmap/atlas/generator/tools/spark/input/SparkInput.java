package org.openstreetmap.atlas.generator.tools.spark.input;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.streaming.Streams;
import org.openstreetmap.atlas.streaming.compression.Decompressor;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Utility tools to load RDDs from elastic file systems (S3 for example), skipping the slowness of
 * the {@link SparkContext} textFile and sequenceFile functions. This is inspired from
 * <a href="http://tech.kinja.com/how-not-to-pull-from-s3-using-apache-spark-1704509219"></a>
 *
 * @author matthieun
 */
public final class SparkInput
{
    private static FileSystemCreator FILE_SYSTEM_CREATOR;
    private static final Logger logger = LoggerFactory.getLogger(SparkInput.class);
    private static final Predicate<String> HIDDEN_FILE_PREDICATE = name -> name.startsWith("_")
            || name.startsWith(".");

    /**
     * Load Atlas files from an input path
     *
     * @param context
     *            The context from Spark
     * @param path
     *            The path to a set of atlas files
     * @return An RDD of Atlas objects. One Atlas object per slice.
     */
    public static JavaRDD<Atlas> atlasFiles(final JavaSparkContext context, final String path)
    {
        return transform(context, path,
                // The first method: case of an elastic file system.
                // For each path, we return one Atlas.
                (BiFunction<Path, Map<String, String>, Iterable<Tuple2<Integer, Atlas>>> & Serializable) (
                        elasticPath, map) ->
                {
                    final Resource resource = getResource(elasticPath, map);
                    final Atlas atlas = new AtlasResourceLoader().load(resource);
                    final List<Atlas> result = new ArrayList<>();
                    if (atlas != null)
                    {
                        // Trick to force a full load of the Atlas here. The Atlas Serializer is
                        // needed for partial loading, but as Spark serializes the Atlas around, it
                        // will lose it, as it is transient.
                        logger.info("Loading Atlas resource {}", resource.getName());
                        atlas.area(0);
                        result.add(atlas);
                    }
                    return Iterables.stream(result)
                            .map(atlasResult -> new Tuple2<>(0, atlasResult));
                }).values();
    }

    /**
     * Get an RDD from a binary file input
     *
     * @param context
     *            The context from Spark
     * @param path
     *            The path to a set of binary files
     * @return An RDD made of pairs of names to values from the binary files read.
     */
    public static JavaPairRDD<String, PortableDataStream> binaryFile(final JavaSparkContext context,
            final String path)
    {
        // Call the transform method
        return transform(context, path,
                // The first method: case of an elastic file system.
                // For each path, we return the single binary file that is at this path.
                // This lambda needs to be serializable.
                (BiFunction<Path, Map<String, String>, Iterable<Tuple2<String, PortableDataStream>>> & Serializable) (
                        elasticPath, map) ->
                {
                    final TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(
                            toHadoop(map), new TaskAttemptID());
                    final long fileLength;
                    try
                    {
                        fileLength = getFileSystemCreator().get(elasticPath.toString(), map)
                                .getFileStatus(elasticPath).getLen();
                    }
                    catch (final IOException e)
                    {
                        throw new CoreException("Unable to get file length for {}",
                                elasticPath.toString(), e);
                    }
                    final CombineFileSplit split = new CombineFileSplit(new Path[] { elasticPath },
                            new long[] { fileLength });
                    final PortableDataStream result = new PortableDataStream(split,
                            taskAttemptContext, 0);
                    return Iterables.from(new Tuple2<>(elasticPath.toString(), result));
                },
                // In case of a non elastic file system, like HDFS, use the standard method.
                otherPath -> context.binaryFiles(otherPath));
    }

    /**
     * Get an RDD from a sequence file input
     *
     * @param <K>
     *            The key type in the sequence file
     * @param <V>
     *            The value type in the sequence file
     * @param context
     *            The context from Spark
     * @param path
     *            The path to a set of sequence files
     * @param sequenceKeyClass
     *            The class to expect for the sequence file keys
     * @param sequenceValueClass
     *            The class to expect for the sequence file values
     * @return An RDD made of pairs of keys to values from the sequence files read.
     */
    public static <K extends Writable, V extends Writable> JavaPairRDD<K, V> sequenceFile(
            final JavaSparkContext context, final String path, final Class<K> sequenceKeyClass,
            final Class<V> sequenceValueClass)
    {
        // Call the transform method
        return transform(context, path,
                // The first method: case of an elastic file system.
                // For each path, we return all the sequence files binary key values
                // This lambda needs to be serializable.
                (BiFunction<Path, Map<String, String>, Iterable<Tuple2<K, V>>> & Serializable) (
                        elasticPath, map) ->
                {
                    try
                    {
                        // Return the Iterable of Key/Value Tuples
                        return () -> new Iterator<Tuple2<K, V>>()
                        {
                            // Make sure the key and value are not null at first. Seems silly.
                            private final K key = ReflectionUtils.newInstance(sequenceKeyClass,
                                    toHadoop(map));
                            private final V value = ReflectionUtils.newInstance(sequenceValueClass,
                                    toHadoop(map));
                            private Boolean hasNext = null;
                            private SequenceFile.Reader sequenceFileReader;

                            @Override
                            public boolean hasNext()
                            {
                                if (this.hasNext == null)
                                {
                                    final Option filePath = SequenceFile.Reader.file(elasticPath);
                                    try
                                    {
                                        this.sequenceFileReader = new SequenceFile.Reader(
                                                toHadoop(map), filePath);
                                    }
                                    catch (final IOException e)
                                    {
                                        throw new CoreException(
                                                "Could not create sequence file reader.", e);
                                    }

                                    // Make sure the sequence file contains the expected
                                    // types
                                    if (sequenceKeyClass != this.sequenceFileReader.getKeyClass())
                                    {
                                        Streams.close(this.sequenceFileReader);
                                        throw new CoreException(
                                                "The sequence file's key is {} whereas the expected class is {}",
                                                this.sequenceFileReader.getKeyClass(),
                                                sequenceKeyClass);
                                    }
                                    if (sequenceValueClass != this.sequenceFileReader
                                            .getValueClass())
                                    {
                                        Streams.close(this.sequenceFileReader);
                                        throw new CoreException(
                                                "The sequence file's value is {} whereas the expected class is {}",
                                                this.sequenceFileReader.getValueClass(),
                                                sequenceValueClass);
                                    }
                                    // Look ahead for the first time.
                                    lookAhead();
                                }
                                return this.hasNext;
                            }

                            @Override
                            public Tuple2<K, V> next()
                            {
                                if (hasNext())
                                {
                                    final Tuple2<K, V> result = new Tuple2<>(this.key, this.value);
                                    lookAhead();
                                    return result;
                                }
                                return null;
                            }

                            private void lookAhead()
                            {
                                try
                                {
                                    // This is where the read happens.
                                    this.hasNext = this.sequenceFileReader.next(this.key,
                                            this.value);
                                    if (!this.hasNext)
                                    {
                                        Streams.close(this.sequenceFileReader);
                                    }
                                }
                                catch (final EOFException e)
                                {
                                    Streams.close(this.sequenceFileReader);
                                    this.hasNext = false;
                                }
                                catch (final Exception e)
                                {
                                    Streams.close(this.sequenceFileReader);
                                    throw new CoreException("Unable to walk through sequence file",
                                            e);
                                }
                            }
                        };
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException("Could not read Sequence File from {}", path, e);
                    }
                },
                // In case of a non elastic file system, like HDFS, use the standard method.
                /* (Function<String, JavaPairRDD<K, V>> & Serializable) */
                otherPath -> context.sequenceFile(otherPath, sequenceKeyClass, sequenceValueClass));
    }

    /**
     * Get an RDD from a Text file input
     *
     * @param context
     *            The context from Spark
     * @param path
     *            The path to open
     * @return A RDD with one {@link String} line per item.
     */
    public static JavaRDD<String> textFile(final JavaSparkContext context, final String path)
    {
        return transform(context, path,
                // The first method: case of an elastic file system.
                // For each path, we return all the lines of the file
                // This lambda needs to be serializable.
                (BiFunction<Path, Map<String, String>, Iterable<Tuple2<Integer, String>>> & Serializable) (
                        elasticPath, map) ->
                {
                    final Resource resource = getResource(elasticPath, map);
                    return Iterables.translate(resource.lines(), line -> new Tuple2<>(0, line));
                },
                // In case of a non elastic file system, like HDFS, use the standard method.
                /* (Function<String, JavaPairRDD<Integer, String>> & Serializable) */
                otherPath -> context.textFile(otherPath)
                        // Here we juggle a bit to maintain the K,V interface, but return just the
                        // values, by creating fake/useless keys.
                        .mapToPair(value -> new Tuple2<>(0, value))).values();
    }

    protected static void setFileSystemCreator(final FileSystemCreator fileSystemCreator)
    {
        SparkInput.FILE_SYSTEM_CREATOR = fileSystemCreator;
    }

    /**
     * Test if a path is coming from an elastic file system, and not from HDFS
     *
     * @param context
     *            The context from Spark
     * @param path
     *            The path considered
     * @return An optional elastic file system if any for this path.
     */
    private static Optional<FileSystem> elasticFileSystem(final JavaSparkContext context,
            final String path)
    {
        final FileSystem fileSystem = fileSystem(context, path);
        // Always return as if it was elastic here:
        return Optional.of(fileSystem);
    }

    private static FileSystem fileSystem(final JavaSparkContext context, final String path)
    {
        final FileSystemCreator creator = getFileSystemCreator();
        return creator.get(path, context.getConf());
    }

    private static FileSystemCreator getFileSystemCreator()
    {
        if (FILE_SYSTEM_CREATOR == null)
        {
            FILE_SYSTEM_CREATOR = new FileSystemCreator();
        }
        return FILE_SYSTEM_CREATOR;
    }

    private static Resource getResource(final Path elasticPath, final Map<String, String> map)
    {
        final InputStreamResource resource;
        try
        {
            // Provide the resource with a Supplier, so the resource can be read multiple times.
            resource = new InputStreamResource(() ->
            {
                try
                {
                    return getFileSystemCreator().get(elasticPath.toString(), map)
                            .open(elasticPath);
                }
                catch (final Exception e)
                {
                    throw new CoreException("Unable to open stream {}", elasticPath, e);
                }

            }).withName(elasticPath.toString());
            if (resource.getName().endsWith(FileSuffix.GZIP.toString()))
            {
                resource.setDecompressor(Decompressor.GZIP);
            }
        }
        catch (final Exception e)
        {
            throw new CoreException("Cannot read {}", elasticPath, e);
        }
        return resource;
    }

    /**
     * Translate a Map configuration to a hadoop {@link Configuration}
     *
     * @param conf
     *            the Map
     * @return the hadoop configuration
     */
    private static Configuration toHadoop(final Map<String, String> conf)
    {
        final Configuration result = new Configuration();
        conf.forEach((key, value) -> result.set(key, value));
        return result;
    }

    /**
     * Translate a Spark conf to a Map
     *
     * @param conf
     *            the spark conf
     * @return the map
     */
    private static Map<String, String> toMap(final SparkConf conf)
    {
        final Map<String, String> result = new HashMap<>();
        for (final Tuple2<String, String> tuple : conf.getAll())
        {
            result.put(tuple._1(), tuple._2());
        }
        return result;
    }

    /**
     * Translate a Map configuration to a Spark {@link SparkConf}
     *
     * @param conf
     *            the Map
     * @return the Spark configuration
     */
    @SuppressWarnings("unused")
    private static SparkConf toSpark(final Map<String, String> conf)
    {
        final SparkConf result = new SparkConf();
        conf.forEach((key, value) -> result.set(key, value));
        return result;
    }

    /**
     * Transform a raw path into the desired RDD. The caller provides what to do to the file to make
     * it an RDD, in the form of functions.
     *
     * @param context
     *            The context from Spark
     * @param path
     *            the raw path
     * @param elasticFunction
     *            Given a {@link Path} for a single blob (not a folder), return an iterable of items
     *            to include in the RDD. Example is all the lines in each blob, in case of a text
     *            blob, or all the binary key/value pairs in a sequence blob. The parameters of this
     *            method need to be serializable...hence the use of a Map for the configuration.
     * @return The prepared RDD.
     */
    private static <K, V> JavaPairRDD<K, V> transform(final JavaSparkContext context,
            final String path,
            final BiFunction<Path, Map<String, String>, Iterable<Tuple2<K, V>>> elasticFunction)
    {
        return transform(context, path, elasticFunction, null);
    }

    /**
     * Transform a raw path into the desired RDD. The caller provides what to do to the file to make
     * it an RDD, in the form of functions.
     *
     * @param context
     *            The context from Spark
     * @param path
     *            the raw path
     * @param elasticFunction
     *            Given a {@link Path} for a single blob (not a folder), return an iterable of items
     *            to include in the RDD. Example is all the lines in each blob, in case of a text
     *            blob, or all the binary key/value pairs in a sequence blob. The parameters of this
     *            method need to be serializable...hence the use of a Map for the configuration.
     * @param defaultFunction
     *            What to return if the path is not an elastic path
     * @return The prepared RDD.
     */
    private static <K, V> JavaPairRDD<K, V> transform(final JavaSparkContext context,
            final String path,
            final BiFunction<Path, Map<String, String>, Iterable<Tuple2<K, V>>> elasticFunction,
            final Function<String, JavaPairRDD<K, V>> defaultFunction)
    {
        final Optional<FileSystem> fileSystemOption = elasticFileSystem(context, path);
        if (fileSystemOption.isPresent() || defaultFunction == null)
        {
            // Case 1. We have an elastic file system. Use the elastic method.
            @SuppressWarnings("resource")
            final FileSystem fileSystem = fileSystemOption.isPresent() ? fileSystemOption.get()
                    : fileSystem(context, path);
            final List<String> files = new ArrayList<>();
            try
            {
                // List the files first, which will make the base for the first RDD
                final FileStatus[] statuses = fileSystem.listStatus(new Path(path));
                for (final FileStatus status : statuses)
                {
                    // Get rid of _SUCCESS files
                    if (HIDDEN_FILE_PREDICATE.negate().test(status.getPath().getName()))
                    {
                        files.add(status.getPath().toUri().toString());
                    }
                }
                // Call "parallelize" to build a simple RDD of file names.
                final JavaRDD<String> paths = context.parallelize(files);
                // Now that all the file names are distributed, we can start reading the inside of
                // the files. Even if the files are large, this will be distributed.
                final Map<String, String> map = toMap(context.getConf());
                return paths.flatMapToPair(elasticPath -> elasticFunction
                        .apply(new Path(elasticPath), map).iterator());
            }
            catch (final Exception e)
            {
                throw new CoreException("Could not list blobs from the file system:\n{}",
                        new StringList(files).join("\n"), e);
            }
        }
        else
        {
            // Case 2. We have a regular DFS, use the default method.
            return defaultFunction.apply(path);
        }
    }

    private SparkInput()
    {
    }
}
