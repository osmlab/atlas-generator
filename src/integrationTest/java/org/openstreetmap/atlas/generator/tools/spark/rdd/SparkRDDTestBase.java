package org.openstreetmap.atlas.generator.tools.spark.rdd;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.openstreetmap.atlas.generator.tools.spark.context.DefaultSparkContextProvider;

/**
 * @author tian_du
 */
public class SparkRDDTestBase extends DefaultSparkContextProvider
{

    private Map<String, String> fileSystemConfig = new HashMap<>();

    private SparkConf configuration = new SparkConf().setMaster("local[*]")
            .setAppName(this.getClass().getCanonicalName());

    private JavaSparkContext sparkContext = null;

    @Before
    public void initialize()
    {
        if (this.sparkContext == null)
        {
            this.sparkContext = new DefaultSparkContextProvider().apply(getConfiguration());
        }
    }

    @After
    public void stopContext()
    {
        if (this.sparkContext != null)
        {
            this.sparkContext.stop();
        }
    }

    protected SparkConf getConfiguration()
    {
        return this.configuration;
    }

    protected Map<String, String> getFileSystemConfiguration()
    {
        return this.fileSystemConfig;
    }

    protected JavaSparkContext getSparkContext()
    {
        return this.sparkContext;
    }

    protected void setConfiguration(final SparkConf configuration)
    {
        this.configuration = configuration;
    }

    protected void setFileSystemConfiguration(final Map<String, String> fileSystemConfiguration)
    {
        this.fileSystemConfig = fileSystemConfiguration;
    }

}
