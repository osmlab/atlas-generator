package org.openstreetmap.atlas.generator.tools.spark.input;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author matthieun
 */
public class SparkInputTest
{
    private static final Logger logger = LoggerFactory.getLogger(SparkInputTest.class);

    private static final String NAME = "test/seqFile.seq";
    private static final String PATH = ResourceFileSystem.SCHEME + "://" + NAME;
    private JavaSparkContext context;

    @Before
    public void setup()
    {
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test SparkInput");
        conf.set(ResourceFileSystem.RESOURCE_FILE_SYSTEM_CONFIGURATION,
                ResourceFileSystem.class.getCanonicalName());
        ResourceFileSystem.addResource(PATH, new InputStreamResource(
                () -> SparkInputTest.class.getResourceAsStream("test.seq")));
        this.context = new JavaSparkContext(conf);
    }

    @After
    public void stop()
    {
        if (this.context != null)
        {
            this.context.stop();
        }
    }

    @Test
    public void testBinaryFileReader()
    {
        // Re-use the same binary file as the one for testing the sequence file
        final JavaPairRDD<String, PortableDataStream> result = SparkInput.binaryFile(this.context,
                PATH);
        Assert.assertEquals(1L, result.count());
        result.collect().forEach(tuple ->
        {
            final String path = tuple._1().toString();
            Assert.assertEquals(PATH, path);
            final PortableDataStream binaryStream = tuple._2();
            final ByteArrayResource gatherer = new ByteArrayResource();
            try (OutputStream outputStream = new BufferedOutputStream(gatherer.write());
                    InputStream inputStream = binaryStream.open())
            {
                IOUtils.copy(inputStream, outputStream);
            }
            catch (final IOException e)
            {
                throw new CoreException("Error copying streams", e);
            }
            final String gathered = gatherer.readAndClose();
            logger.info("{}", gathered);
            Assert.assertEquals(181, gatherer.length());
            Assert.assertTrue(gathered.contains("test.txt"));
        });
    }

    @Test
    public void testSequenceFileReader()
    {
        final JavaPairRDD<Text, BytesWritable> result = SparkInput.sequenceFile(this.context, PATH,
                Text.class, BytesWritable.class);
        result.foreach(tuple ->
        {
            logger.info(tuple._1().toString() + " -> " + tuple._2().toString());
        });
        Assert.assertEquals(1L, result.count());
    }
}
