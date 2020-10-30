package org.openstreetmap.atlas.generator;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for AtlasDataFrame.
 * 
 * @author jamesgage
 */
public class AtlasDataFrameTest
{
    private static final Logger logger = LoggerFactory.getLogger(AtlasDataFrameTest.class);

    @Test
    public void atlasDataFrameTest()
    {
        final Resource file = new InputStreamResource(
                () -> AtlasDataFrameTest.class.getResourceAsStream("Alcatraz.atlas.txt"));
        final AtlasResourceLoader loader = new AtlasResourceLoader();
        final Atlas atlas = loader.load(file);
        logger.info(atlas.edges().toString());

        final ArrayList<Atlas> atlasList = new ArrayList<>();
        atlasList.add(atlas);

        final SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("DataFrame Test");
        sparkConf.setMaster("local");
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        final JavaRDD<Atlas> atlasJavaRDD = javaSparkContext.parallelize(atlasList,
                atlasList.size());

        final Dataset<Row> areaDataframe = AtlasDataFrame.atlasAreasToDataFrame(atlasJavaRDD,
                javaSparkContext);

        areaDataframe.show();
        Assert.assertEquals(areaDataframe.first().get(0), "24433389000000");
        javaSparkContext.close();
    }
}
