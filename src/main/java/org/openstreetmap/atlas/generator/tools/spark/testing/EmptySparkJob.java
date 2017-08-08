package org.openstreetmap.atlas.generator.tools.spark.testing;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.scalars.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author matthieun
 */
public class EmptySparkJob extends SparkJob
{
    private static final long serialVersionUID = -687554634010359849L;
    private static final Logger logger = LoggerFactory.getLogger(EmptySparkJob.class);

    public static final Switch<Integer> SIZE = new Switch<>("size", "The size of the RDD",
            value -> Integer.parseInt(value), Optionality.REQUIRED);
    public static final Switch<Integer> LENGTH = new Switch<>("lengthInSeconds",
            "The length in Seconds of each task.", value -> Integer.parseInt(value),
            Optionality.REQUIRED);

    public static void main(final String[] args)
    {
        new EmptySparkJob().run(args);
    }

    @Override
    public String getName()
    {
        return "EmptySparkJob";
    }

    @Override
    public void start(final CommandMap command)
    {
        final int size = (int) command.get(SIZE);
        final int length = (int) command.get(LENGTH);
        final String output = (String) command.get(OUTPUT);
        logger.info("Starting empty spark job with size {} and length {} seconds.", size, length);
        final List<Integer> input = new ArrayList<>();
        for (int value = 0; value < size; value++)
        {
            input.add(value);
        }
        final JavaRDD<Integer> rdd = getContext().parallelize(input);
        final JavaRDD<Boolean> result = rdd.map(value ->
        {
            for (int counter = 0; counter < length; counter++)
            {
                Duration.ONE_SECOND.sleep();
                logger.info("RDD part {} slept {} times.", value, counter);
            }
            return true;
        });
        result.saveAsTextFile(output);
    }

    @Override
    protected SwitchList switches()
    {
        return super.switches().with(SIZE, LENGTH);
    }
}
