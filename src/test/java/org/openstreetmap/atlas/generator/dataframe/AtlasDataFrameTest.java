package org.openstreetmap.atlas.generator.dataframe;

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

        // Test IDs
        final Dataset<Row> areaDataframe = AtlasDataFrame.atlasAreasToDataFrame(atlasJavaRDD,
                javaSparkContext);
        final Dataset<Row> edgeDataframe = AtlasDataFrame.atlasEdgesToDataFrame(atlasJavaRDD,
                javaSparkContext);
        final Dataset<Row> nodeDataframe = AtlasDataFrame.atlasNodesToDataFrame(atlasJavaRDD,
                javaSparkContext);
        final Dataset<Row> lineDataframe = AtlasDataFrame.atlasLinesToDataFrame(atlasJavaRDD,
                javaSparkContext);
        final Dataset<Row> pointDataframe = AtlasDataFrame.atlasPointsToDataFrame(atlasJavaRDD,
                javaSparkContext);
        final Dataset<Row> relationDataframe = AtlasDataFrame
                .atlasRelationsToDataFrame(atlasJavaRDD, javaSparkContext);

        Assert.assertEquals("24433389000000", areaDataframe.first().get(0));
        Assert.assertEquals("27989500000000", edgeDataframe.first().get(0));
        Assert.assertEquals("307351652000000", nodeDataframe.first().get(0));
        Assert.assertEquals("99202295000000", lineDataframe.first().get(0));
        Assert.assertEquals("4553243887000000", pointDataframe.first().get(0));
        Assert.assertEquals("9451753000000", relationDataframe.first().get(0));

        // Test tags
        final scala.collection.immutable.HashMap areaTags = (scala.collection.immutable.HashMap) areaDataframe
                .first().get(3);
        final scala.collection.immutable.HashMap edgeTags = (scala.collection.immutable.HashMap) edgeDataframe
                .first().get(8);
        final scala.collection.immutable.HashMap lineTags = (scala.collection.immutable.HashMap) lineDataframe
                .first().get(4);
        final scala.collection.immutable.HashMap nodeTags = (scala.collection.immutable.HashMap) nodeDataframe
                .first().get(4);
        final scala.collection.immutable.HashMap pointTags = (scala.collection.immutable.HashMap) pointDataframe
                .first().get(2);
        final scala.collection.immutable.HashMap relationTags = (scala.collection.immutable.HashMap) relationDataframe
                .first().get(5);
        Assert.assertEquals("(last_edit_user_name,Edward)", areaTags.iterator().next().toString());
        Assert.assertEquals("(payment:clipper,no)", edgeTags.iterator().next().toString());
        Assert.assertEquals("(last_edit_user_name,phut)", lineTags.iterator().next().toString());
        Assert.assertEquals("(last_edit_user_name,KindredCoda)",
                nodeTags.iterator().next().toString());
        Assert.assertEquals("(last_edit_user_name,KindredCoda)",
                pointTags.iterator().next().toString());
        Assert.assertEquals("(last_edit_user_name,StenSoft)",
                relationTags.iterator().next().toString());

        // Test size

        Assert.assertEquals(32, areaDataframe.count());
        Assert.assertEquals(130, edgeDataframe.count());
        Assert.assertEquals(10, lineDataframe.count());
        Assert.assertEquals(56, nodeDataframe.count());
        Assert.assertEquals(26, pointDataframe.count());
        Assert.assertEquals(1, relationDataframe.count());

        javaSparkContext.close();
    }
}
