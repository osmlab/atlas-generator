package org.openstreetmap.atlas.generator.tools.spark.converters;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * @author matthieun
 */
public final class ConfigurationConverter
{
    public static Configuration mapToHadoopConfiguration(final Map<String, String> map)
    {
        final Configuration result = new Configuration();
        map.forEach(result::set);
        return result;
    }

    private ConfigurationConverter()
    {
    }
}
