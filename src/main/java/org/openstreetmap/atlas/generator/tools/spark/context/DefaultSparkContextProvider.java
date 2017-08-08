package org.openstreetmap.atlas.generator.tools.spark.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Default {@link SparkContextProvider} that provides a {@link JavaSparkContext}
 *
 * @author matthieun
 */
public class DefaultSparkContextProvider implements SparkContextProvider
{
    @Override
    public JavaSparkContext apply(final SparkConf configuration)
    {
        return new JavaSparkContext(configuration);
    }
}
