package org.openstreetmap.atlas.generator.tools.spark.converters;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;

/**
 * {@link StringConverter} that does the parsing of additional Spark context string values.
 *
 * @author matthieun
 */
public class SparkOptionsStringConverter implements StringConverter<Map<String, String>>
{
    @Override
    public Map<String, String> convert(final String string)
    {
        final Map<String, String> result = new HashMap<>();
        if ("".equals(string))
        {
            return result;
        }
        final StringList split = StringList.split(string, ",");
        split.forEach(pair ->
        {
            // Split only once
            StringList arrowSplit = StringList.split(pair, "->", 2);
            if (arrowSplit.size() <= 1)
            {
                // Split only once
                arrowSplit = StringList.split(pair, "=", 2);
            }
            if (arrowSplit.size() > 1)
            {
                try
                {
                    result.put(arrowSplit.get(0), arrowSplit.get(1));
                }
                catch (final Exception e)
                {
                    throw new CoreException("Unable to parse additional Spark Option: {}", pair, e);
                }
            }
            else if (arrowSplit.size() == 1)
            {
                result.put(arrowSplit.get(0), "");
            }
            else
            {
                // Empty spark option, do nothing.
            }
        });
        return result;
    }
}
