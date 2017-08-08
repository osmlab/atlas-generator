package org.openstreetmap.atlas.generator.tools.filesystem.converters;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.openstreetmap.atlas.utilities.conversion.TwoWayConverter;

import scala.Tuple2;

/**
 * Convert back and forth between Spark's configuration and Hadoop's configuration.
 *
 * @author matthieun
 */
public class SparkConfToHadoopConfigurationConverter
        implements TwoWayConverter<SparkConf, Configuration>, Serializable
{
    private static final long serialVersionUID = -4539133054876396057L;

    @Override
    public SparkConf backwardConvert(final Configuration configuration)
    {
        final SparkConf conf = new SparkConf();
        configuration.forEach(entry ->
        {
            conf.set(entry.getKey(), entry.getValue());
        });
        return conf;
    }

    @Override
    public Configuration convert(final SparkConf conf)
    {
        final Configuration configuration = new Configuration();
        for (final Tuple2<String, String> tuple : conf.getAll())
        {
            configuration.set(tuple._1(), tuple._2());
        }
        return configuration;
    }

}
