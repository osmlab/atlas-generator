package org.openstreetmap.atlas.mutator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.spark.context.DefaultSparkContextProvider;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.Heading;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.utilities.scalars.Distance;
import org.openstreetmap.atlas.utilities.scalars.Ratio;

import scala.Tuple2;

/**
 * @author matthieun
 */
public class AtlasMutatorDriverIntegrationTest extends AbstractAtlasMutatorIntegrationTest
{
    public static final String SHARDING_PATH = "resource://" + SHARDING_FILE_NAME;
    public static final String COUNTRY = "XYZ";
    public static final CountryShard Z_8_X_130_Y_98 = new CountryShard(COUNTRY,
            SlippyTile.forName("8-130-98"));
    public static final CountryShard Z_8_X_131_Y_97 = new CountryShard(COUNTRY,
            SlippyTile.forName("8-131-97"));
    public static final CountryShard Z_8_X_131_Y_98 = new CountryShard(COUNTRY,
            SlippyTile.forName("8-131-98"));
    public static final CountryShard Z_9_X_261_Y_195 = new CountryShard(COUNTRY,
            SlippyTile.forName("9-261-195"));

    @Rule
    public final AtlasMutatorIntegrationTestRule rule = new AtlasMutatorIntegrationTestRule();

    private Atlas shard1 = null;
    private Atlas shard2 = null;
    private Atlas shard3 = null;

    @Override
    public List<String> arguments()
    {
        return new ArrayList<>();
    }

    @Before
    public void setup()
    {
        this.shard1 = this.rule.getZ9X261Y195();
        this.shard2 = this.rule.getZ8X131Y97();
        this.shard3 = this.rule.getZ8X131Y98();
        ResourceFileSystem.clear();
        addResource(SHARDING_PATH, "tree-6-14-100000.txt");
    }

    @Test
    public void testGetAtlasGroupsRDD()
    {
        final JavaSparkContext context = getContext();
        try
        {
            final Sharding sharding = AtlasSharding.forString("dynamic@" + SHARDING_PATH,
                    ResourceFileSystem.simpleconfiguration());

            final List<Tuple2<CountryShard, PackedAtlas>> atlasRDDSource = new ArrayList<>();
            atlasRDDSource.add(new Tuple2<>(Z_8_X_131_Y_97, (PackedAtlas) this.shard2));
            atlasRDDSource.add(new Tuple2<>(Z_8_X_131_Y_98, (PackedAtlas) this.shard3));
            atlasRDDSource.add(new Tuple2<>(Z_9_X_261_Y_195, (PackedAtlas) this.shard1));
            atlasRDDSource.add(new Tuple2<>(Z_8_X_130_Y_98, null));
            final JavaPairRDD<CountryShard, PackedAtlas> atlasRDD = context
                    .parallelizePairs(atlasRDDSource);
            final Function<CountryShard, Set<CountryShard>> shardExplorer = (Serializable & Function<CountryShard, Set<CountryShard>>) countryShard ->
            {
                final Set<CountryShard> result = new HashSet<>();
                sharding.shardsCovering(
                        countryShard.bounds().center().shiftAlongGreatCircle(Heading.EAST,
                                countryShard.bounds().width().onEarth().scaleBy(Ratio.HALF)
                                        .add(Distance.ONE_METER)))
                        .forEach(shard -> result.add(new CountryShard(COUNTRY, shard)));
                result.add(countryShard);
                return result;
            };
            final JavaPairRDD<CountryShard, Map<CountryShard, PackedAtlas>> atlasGroupsRDD = AtlasMutatorDriver
                    .getAtlasGroupsRDD("DriverTest", atlasRDDSource.size(), atlasRDD, shardExplorer,
                            Optional.empty());
            final Map<CountryShard, Map<CountryShard, PackedAtlas>> result = atlasGroupsRDD
                    .collectAsMap();

            Assert.assertEquals(4, result.size());

            final Map<CountryShard, PackedAtlas> mZ8X131Y97 = result.get(Z_8_X_131_Y_97);
            Assert.assertNotNull(mZ8X131Y97);
            Assert.assertEquals(1, mZ8X131Y97.size());
            Assert.assertNotNull(mZ8X131Y97.get(Z_8_X_131_Y_97));

            final Map<CountryShard, PackedAtlas> mZ8X131Y98 = result.get(Z_8_X_131_Y_98);
            Assert.assertNotNull(mZ8X131Y98);
            Assert.assertEquals(1, mZ8X131Y98.size());
            Assert.assertNotNull(mZ8X131Y98.get(Z_8_X_131_Y_98));

            final Map<CountryShard, PackedAtlas> mZ9X261Y195 = result.get(Z_9_X_261_Y_195);
            Assert.assertNotNull(mZ9X261Y195);
            Assert.assertEquals(2, mZ9X261Y195.size());
            Assert.assertNotNull(mZ9X261Y195.get(Z_9_X_261_Y_195));
            Assert.assertNotNull(mZ9X261Y195.get(Z_8_X_131_Y_97));

            final Map<CountryShard, PackedAtlas> mZ8X130Y98 = result.get(Z_8_X_130_Y_98);
            Assert.assertNotNull(mZ8X130Y98);
            Assert.assertEquals(1, mZ8X130Y98.size());
            // This shard is initially empty, it only gets the neighboring shard.
            Assert.assertNotNull(mZ8X130Y98.get(Z_8_X_131_Y_98));
        }
        finally
        {
            context.close();
        }
    }

    private JavaSparkContext getContext()
    {
        final SparkConf sparkConfiguration = new SparkConf()
                .setAppName(AtlasMutatorDriverIntegrationTest.class.getSimpleName());
        // TODO replace with inclusive language once
        // https://issues.apache.org/jira/browse/SPARK-32333 is completed
        sparkConfiguration.setMaster("local");
        return new DefaultSparkContextProvider().apply(sparkConfiguration);
    }
}
