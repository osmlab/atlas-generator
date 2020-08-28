package org.openstreetmap.atlas.generator;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtlasDataFrameTest
{
    private static final Logger logger = LoggerFactory.getLogger(AtlasDataFrameTest.class);

    @Test
    public void atlasDataFrameTest()
    {
        final Resource file = new InputStreamResource(
                () -> new File("/Users/jamesgage/Desktop/scratch/dfTesting/DMA_9-168-233.atlas")
                        .read());
        final Atlas atlas = PackedAtlas.load(file);
        logger.info(atlas.metaData().toReadableString());

        final ArrayList<Atlas> atlasList = new ArrayList<>();
        atlasList.add(atlas);

        final SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("DataFrame Test");
        sparkConf.setMaster("local");
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // final JavaRDD<Atlas> atlasJavaRDD = atlasRDD.toJavaRDD();
        final JavaRDD<Atlas> atlasJavaRDD = javaSparkContext.parallelize(atlasList,
                atlasList.size());

        final AtlasDataFrame atlasDataFrame = new AtlasDataFrame();

        final Dataset<Row> areaDataframe = atlasDataFrame.atlasAreasToDataFrame(atlasJavaRDD,
                javaSparkContext);

        areaDataframe.show();
    }
}
