package org.openstreetmap.atlas.generator.tools.spark.converters;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

import scala.Tuple2;

/**
 * @author matthieun
 */
public final class ConfigurationConverter
{
    public static SparkConf hadoopToSparkConfiguration(final Configuration hadoopConfiguration)
    {
        final SparkConf result = new SparkConf();
        hadoopConfiguration.forEach(entry -> result.set(entry.getKey(), entry.getValue()));
        return result;
    }

    public static Configuration mapToHadoopConfiguration(final Map<String, String> map)
    {
        final Configuration result = new Configuration();
        map.forEach(result::set);
        return result;
    }

    public static SparkConf mapToSparkConfiguration(final Map<String, String> map)
    {
        final SparkConf result = new SparkConf();
        map.forEach(result::set);
        return result;
    }

    public static Configuration sparkToHadoopConfiguration(final SparkConf sparkConfiguration)
    {
        final Configuration result = new Configuration();
        final Tuple2<String, String>[] all = sparkConfiguration.getAll();
        for (final Tuple2<String, String> tuple : all)
        {
            result.set(tuple._1(), tuple._2());
        }
        return result;
    }

    private ConfigurationConverter()
    {
    }
}
