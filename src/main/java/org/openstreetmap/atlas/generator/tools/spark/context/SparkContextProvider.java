package org.openstreetmap.atlas.generator.tools.spark.context;

import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Function that provides a {@link SparkContext}
 *
 * @author matthieun
 */
public interface SparkContextProvider extends Function<SparkConf, JavaSparkContext>
{
}
