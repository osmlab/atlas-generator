package org.openstreetmap.atlas.generator.persistence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompletePoint;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

import scala.Tuple2;

/**
 * @author matthieun
 */
public class FeatureChangeOutputFormatIntegrationTest
{
    private static class TestSparkJob extends SparkJob
    {
        private static final long serialVersionUID = -4875961726986591317L;

        @Override
        public String getName()
        {
            return "FeatureChangeOutputFormatIntegrationTest Spark Job";
        }

        @SuppressWarnings("unchecked")
        @Override
        public void start(final CommandMap command)
        {
            getContext().parallelize(StringList
                    .split("{\"country\":\"ABC\",\"shard\":\"1-2-3\",\"metadata\":{}};"
                            + "{\"country\":\"DEF\",\"shard\":\"4-5-6\",\"metadata\":{}};"
                            + "{\"country\":\"GHI\",\"shard\":\"7-8-9\",\"metadata\":{}}", ";")
                    .stream().collect(Collectors.toList())).mapToPair(key ->
                    {
                        final List<FeatureChange> list = new ArrayList<>();
                        list.add(FeatureChange
                                .remove(new CompletePoint(123L, Location.COLOSSEUM, null, null)));
                        list.add(FeatureChange.remove(
                                new CompletePoint(456L, Location.EIFFEL_TOWER, null, null)));
                        return new Tuple2(key, list);
                    }).saveAsHadoopFile((String) command.get(OUTPUT), Text.class, Atlas.class,
                            MultipleFeatureChangeOutputFormat.class, new JobConf(configuration()));
        }
    }

    private static final String OUTPUT = "resource://test/output";
    private static final String PATH_1 = OUTPUT + "/ABC/ABC_1-2-3.geojson.gz";
    private static final String PATH_2 = OUTPUT + "/DEF/DEF_4-5-6.geojson.gz";
    private static final String PATH_3 = OUTPUT + "/GHI/GHI_7-8-9.geojson.gz";

    @Test
    public void testSave() throws IOException
    {
        final StringList arguments = new StringList();
        arguments.add("-master=local");
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add(
                "-sparkOptions=fs.resource.impl=" + ResourceFileSystem.class.getCanonicalName());

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }

        ResourceFileSystem.clear();
        new TestSparkJob().runWithoutQuitting(args);
        ResourceFileSystem.printContents();
        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem())
        {
            Assert.assertTrue(resourceFileSystem.exists(new Path(PATH_1)));
            Assert.assertEquals(2, Iterables.size(
                    SparkJob.resource(PATH_1, ResourceFileSystem.simpleconfiguration()).lines()));
            Assert.assertTrue(resourceFileSystem.exists(new Path(PATH_2)));
            Assert.assertEquals(2, Iterables.size(
                    SparkJob.resource(PATH_2, ResourceFileSystem.simpleconfiguration()).lines()));
            Assert.assertTrue(resourceFileSystem.exists(new Path(PATH_3)));
            Assert.assertEquals(2, Iterables.size(
                    SparkJob.resource(PATH_3, ResourceFileSystem.simpleconfiguration()).lines()));
        }
    }
}
