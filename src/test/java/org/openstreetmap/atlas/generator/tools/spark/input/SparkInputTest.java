package org.openstreetmap.atlas.generator.tools.spark.input;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
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
