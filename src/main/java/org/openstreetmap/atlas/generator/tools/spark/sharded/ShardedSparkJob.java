package org.openstreetmap.atlas.generator.tools.spark.sharded;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedPolygon;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMapArchiver;
import org.openstreetmap.atlas.geography.converters.jts.JtsPolygonConverter;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.ResourceCloseable;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.maps.MultiMap;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Spark job that takes care of reading the sharding and country list and starts with a
 * {@link JavaPairRDD} of CountryName to Shard
 *
 * @author matthieun
 */
public abstract class ShardedSparkJob extends SparkJob
{
    private static final long serialVersionUID = -5341594024010883883L;
    private static final Logger logger = LoggerFactory.getLogger(ShardedSparkJob.class);

    public static final Switch<StringList> COUNTRIES = new Switch<>("countries",
            "Comma separated list of countries to be included in the final Atlas",
            value -> StringList.split(value, ","), Optionality.REQUIRED);
    public static final Switch<String> SHARDING_TYPE = new Switch<>("sharding",
            "The sharding definition.", StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<String> COUNTRY_SHAPES = new Switch<>("countryShapes",
            "Shape file containing the countries", StringConverter.IDENTITY, Optionality.REQUIRED);

    private static PairFlatMapFunction<String, String, Shard> countryToShards(
            final CountryBoundaryMap worldBoundaries, final Sharding sharding)
    {
        return countryName ->
        {
            // For each country boundary
            final List<PreparedPolygon> boundaries = worldBoundaries.getCountryNameToBoundaryMap()
                    .get(countryName);

            // Handle missing boundaries case
            if (boundaries == null)
            {
                logger.error("No boundaries found for country {}!", countryName);
                return new ArrayList<Tuple2<String, Shard>>().iterator();
            }

            logger.info("Generating shards for country {}", countryName);
            final Set<Shard> shards = new HashSet<>();
            for (final PreparedPolygon boundary : boundaries)
            {
                final JtsPolygonConverter converter = new JtsPolygonConverter();
                shards.addAll(Iterables.asList(Iterables.filter(
                        sharding.shards(
                                converter.backwardConvert((Polygon) boundary.getGeometry())),
                        shard -> boundary.overlaps(converter.convert(shard.bounds())))));
            }
            // Assign the country name / shard couples to the countryShards list to be
            // parallelized
            final List<Tuple2<String, Shard>> countryShards = new ArrayList<>();
            shards.forEach(shard -> countryShards.add(new Tuple2<>(countryName, shard)));
            return countryShards.iterator();
        };
    }

    @Override
    public void start(final CommandMap command)
    {
        final StringList countries = (StringList) command.get(COUNTRIES);
        final String shardingType = (String) command.get(SHARDING_TYPE);
        final Sharding sharding = AtlasSharding.forString(shardingType, configuration());
        final String countryShapes = (String) command.get(COUNTRY_SHAPES);
        logger.info("Reading country boundaries from {}", countryShapes);
        final CountryBoundaryMap worldBoundaries;
        try (ResourceCloseable resource = resource(countryShapes))
        {
            worldBoundaries = new CountryBoundaryMapArchiver().read(resource);
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not read {}", countryShapes, e);
        }
        logger.info("Done Reading {} country boundaries from {}", worldBoundaries.size(),
                countryShapes);

        // The code below is as parallel as there are countries...
        final JavaRDD<String> countriesRDD = getContext().parallelize(Iterables.asList(countries),
                countries.size());

        // Stage 0 (parallelize on countries)
        final JavaPairRDD<String, Shard> preCountryShardsRDD = countriesRDD
                .flatMapToPair(countryToShards(worldBoundaries, sharding));

        // Collect The country to shard map
        final List<Tuple2<String, Shard>> countryShards = preCountryShardsRDD.collect();
        final MultiMap<String, Shard> countryToShardMap = new MultiMap<>();
        countryShards.forEach(tuple -> countryToShardMap.add(tuple._1(), tuple._2()));

        // Call the rest!
        start2(command, sharding, countryToShardMap);
    }

    public abstract void start2(CommandMap command, Sharding sharding,
            MultiMap<String, Shard> countryToShardMap);

    @Override
    public SwitchList switches()
    {
        return super.switches().with(COUNTRIES, SHARDING_TYPE, COUNTRY_SHAPES);
    }
}
