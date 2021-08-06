package org.openstreetmap.atlas.generator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.json.PersistenceJsonParser;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.PolyLine;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.ChangeAtlas;
import org.openstreetmap.atlas.geography.atlas.change.ChangeBuilder;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.change.description.ChangeDescriptorType;
import org.openstreetmap.atlas.geography.atlas.change.diff.AtlasDiff;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteRelation;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.atlas.items.ItemType;
import org.openstreetmap.atlas.geography.atlas.items.Relation;
import org.openstreetmap.atlas.geography.atlas.items.Relation.Ring;
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.raw.sectioning.AtlasSectionProcessor;
import org.openstreetmap.atlas.geography.atlas.raw.slicing.RawAtlasSlicer;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.geography.atlas.statistics.Counter;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.converters.MultiplePolyLineToMultiPolygonConverter;
import org.openstreetmap.atlas.geography.converters.jts.JtsMultiPolygonToMultiPolygonConverter;
import org.openstreetmap.atlas.geography.converters.jts.JtsPolygonConverter;
import org.openstreetmap.atlas.geography.converters.jts.JtsPrecisionManager;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.ConfiguredFilter;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Utility class for {@link AtlasGenerator}.
 *
 * @author matthieun
 * @author mgostintsev
 * @author samg
 */
public final class AtlasGeneratorHelper implements Serializable
{
    /**
     * @author matthieun
     */
    protected static class NamedAtlasStatistics implements Serializable
    {
        private static final long serialVersionUID = 1593790111775268766L;
        private final String name;
        private final AtlasStatistics atlasStatistics;

        public NamedAtlasStatistics(final String name, final AtlasStatistics atlasStatistics)
        {
            this.name = name;
            this.atlasStatistics = atlasStatistics;
        }

        public AtlasStatistics getAtlasStatistics()
        {
            return this.atlasStatistics;
        }

        public String getName()
        {
            return this.name;
        }
    }

    public static final String STARTED_MESSAGE = "Starting task {} for shard {}";
    public static final String FINISHED_MESSAGE = "Finished task {} for shard {} in {}";
    public static final String ERROR_MESSAGE = "Error during task {} for shard {} :";

    private static final long serialVersionUID = 1300098384789754747L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasGeneratorHelper.class);

    private static final AtlasResourceLoader ATLAS_LOADER = new AtlasResourceLoader();

    @SuppressWarnings("unchecked")
    public static Function<Shard, Optional<Atlas>> atlasFetcher(
            final HadoopAtlasFileCache lineSlicedSubAtlasCache, final Atlas initialShardAtlas,
            final CountryBoundaryMap boundaries, final String countryBeingSliced,
            final Shard initialShard)
    {
        // & Serializable is very important as that function will be passed around by Spark, and
        // functions are not serializable by default.
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            final StringList countriesForShardList = boundaries
                    .countryCodesOverlappingWith(shard.bounds());
            final Set<String> countriesForShard = new HashSet<>();
            final AtlasResourceLoader loader = new AtlasResourceLoader();
            countriesForShardList.forEach(countriesForShard::add);

            final Set<Atlas> atlases = new HashSet<>();
            // Multi-atlas all remaining sliced water relation data together and return that
            countriesForShard.forEach(country ->
            {
                if (initialShard.equals(shard) && countryBeingSliced.equals(country))
                {
                    logger.debug(
                            "While slicing {}, adding initial atlas for shard {} and country {}",
                            countryBeingSliced, shard, country);
                    atlases.add(initialShardAtlas);
                }
                else
                {
                    final Optional<Resource> cachedAtlas = lineSlicedSubAtlasCache.get(country,
                            shard);
                    if (cachedAtlas.isPresent())
                    {
                        logger.debug(
                                "{}: Cache hit, loading sliced subAtlas for Shard {} and country {}",
                                countryBeingSliced, shard, country);
                        atlases.add(loader.load(cachedAtlas.get()));
                    }
                }
            });
            return atlases.isEmpty() ? Optional.empty()
                    : Optional.ofNullable(new MultiAtlas(atlases));
        };
    }

    @SuppressWarnings("unchecked")
    public static Function<Shard, Optional<Atlas>> atlasFetcher(
            final HadoopAtlasFileCache subAtlasCache, final HadoopAtlasFileCache atlasCache,
            final String countryBeingSliced, final Shard initialShard)
    {
        // & Serializable is very important as that function will be passed around by Spark, and
        // functions are not serializable by default.
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            final Optional<Resource> cachedInitialShardResource;
            // If this is the initial shard, load from full Atlas cache, not subAtlas cache
            if (shard.equals(initialShard))
            {
                cachedInitialShardResource = atlasCache.get(countryBeingSliced, shard);
            }
            else
            {
                // Otherwise, load from subatlas cache
                cachedInitialShardResource = subAtlasCache.get(countryBeingSliced, shard);
            }
            if (!cachedInitialShardResource.isPresent())
            {
                logger.error("{}: No Atlas file found for initial Shard {}!", countryBeingSliced,
                        shard);
                return Optional.empty();
            }
            return Optional.ofNullable(ATLAS_LOADER.load(cachedInitialShardResource.get()));
        };
    }

    public static Set<LineString> convertMultiPolygonToLineCollection(
            final MultiPolygon multipolygon)
    {
        final Set<LineString> linestrings = new HashSet<>();
        for (int i = 0; i < multipolygon.getNumGeometries(); i++)
        {
            final Polygon part = (Polygon) multipolygon.getGeometryN(i);
            linestrings.add(part.getExteriorRing());
            for (int j = 0; j < part.getNumInteriorRing(); j++)
            {
                linestrings.add(part.getInteriorRingN(j));
            }
        }
        return linestrings;
    }

    /**
     * This function takes a given tuple of shard and atlas to the set of multipolygons that were
     * assembled for it. It will then apply each of those relation multipolygons as feature changes
     * and save the final atlas with the geometries serialized
     *
     * @return function that does the above
     */
    protected static PairFunction<Tuple2<String, Tuple2<Atlas, org.apache.spark.api.java.Optional<Set<Tuple2<Long, MultiPolygon>>>>>, String, Atlas> augmentAtlas()
    {
        return (Serializable & PairFunction<Tuple2<String, Tuple2<Atlas, org.apache.spark.api.java.Optional<Set<Tuple2<Long, MultiPolygon>>>>>, String, Atlas>) tuple ->
        {
            final Atlas rawAtlas = tuple._2._1;
            if (!tuple._2()._2.isPresent())
            {
                return new Tuple2<>(tuple._1, rawAtlas);
            }
            final Set<Tuple2<Long, MultiPolygon>> relationGeometryPairs = tuple._2._2.get();
            final Set<FeatureChange> changes = new HashSet<>();
            for (final Tuple2<Long, MultiPolygon> relationGeometryPair : relationGeometryPairs)
            {
                if (rawAtlas.relation(relationGeometryPair._1) != null)
                {
                    final CompleteRelation updated = CompleteRelation
                            .from(rawAtlas.relation(relationGeometryPair._1));
                    updated.withMultiPolygonGeometry(relationGeometryPair._2);
                    changes.add(FeatureChange.add(updated, rawAtlas));
                }
            }
            if (changes.isEmpty())
            {
                return new Tuple2<>(tuple._1, rawAtlas);
            }
            else
            {
                final ChangeBuilder builder = new ChangeBuilder().addAll(changes);
                final ChangeAtlas changeAtlas = new ChangeAtlas(rawAtlas, builder.get());
                return new Tuple2<>(tuple._1, changeAtlas.cloneToPackedAtlas());
            }
        };
    }

    /**
     * This function takes a given tuple of relation id to member geometries and returns a single
     * tuple of relation id to assembled geometry
     *
     * @return function that does the above
     */
    protected static PairFunction<Tuple2<Long, Map<Ring, Iterable<PolyLine>>>, Long, MultiPolygon> buildRelationGeometries()
    {
        return (Serializable & PairFunction<Tuple2<Long, Map<Ring, Iterable<PolyLine>>>, Long, MultiPolygon>) pair ->
        {
            final Long relationId = pair._1;
            logger.error("Relation {} building with member count {}", relationId,
                    pair._2.entrySet().size());
            final Map<Ring, Iterable<PolyLine>> outersInnersMap = pair._2;
            try
            {
                return new Tuple2<>(relationId,
                        new JtsMultiPolygonToMultiPolygonConverter()
                                .backwardConvert(new MultiplePolyLineToMultiPolygonConverter()
                                        .convert(outersInnersMap)));
            }
            catch (final Exception exc)
            {
                logger.error("Couldn't build geometry for relation {}", relationId, exc);
                return new Tuple2<>(relationId, null);
            }
        };
    }

    /**
     * @param sparkContext
     *            Spark context (or configuration) as a key-value map
     * @param previousOutputForDelta
     *            Previous Atlas generation delta output location
     * @return A Spark {@link PairFlatMapFunction} that takes a tuple of a country shard name and
     *         atlas file and returns all the {@link AtlasDiff} for the country
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, List<FeatureChange>> computeAtlasDiff(
            final Map<String, String> sparkContext, final String previousOutputForDelta)
    {
        return tuple ->
        {
            final String countryShardName = getCountryShard(tuple._1()).getName();
            final Atlas current = tuple._2();
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.DIFFS.getDescription(),
                    countryShardName);
            final Time start = Time.now();
            final Optional<Atlas> alter = new AtlasLocator(
                    sparkContext)
                            .atlasForShard(
                                    SparkFileHelper.combine(previousOutputForDelta,
                                            StringList.split(countryShardName,
                                                    Shard.SHARD_DATA_SEPARATOR).get(0)),
                                    countryShardName);
            if (!alter.isPresent())
            {
                logger.error("No atlas found for {}!", countryShardName);
                return new Tuple2<>(tuple._1(), null);
            }

            final Optional<Change> diffChange = new AtlasDiff(alter.get(), current)
                    .generateChange();
            final List<FeatureChange> diffsList = new ArrayList<>();
            if (diffChange.isPresent())
            {
                diffsList.addAll(diffChange.get().changes().collect(Collectors.toList()));
                final Map<ItemType, Map<ChangeDescriptorType, Map<String, AtomicLong>>> tagMap = diffChange
                        .get().tagCountMap();
                for (final ItemType itemType : ItemType.values())
                {
                    for (final ChangeDescriptorType changeDescriptorType : ChangeDescriptorType
                            .values())
                    {
                        tagMap.get(itemType).get(changeDescriptorType).entrySet()
                                .forEach(entry -> logger.info(
                                        "AtlasDiff Tag Summary: {} {} tag {} for {} {}",
                                        countryShardName, changeDescriptorType, entry.getKey(),
                                        entry.getValue(), itemType));
                        final long count = diffsList.stream()
                                .filter(diff -> diff.getItemType().equals(itemType)
                                        && diff.explain().getChangeDescriptorType()
                                                .equals(changeDescriptorType))
                                .count();
                        logger.info("AtlasDiff Change Summary: {} {} {} {}", countryShardName,
                                changeDescriptorType, count, itemType);
                    }
                }
            }
            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.DIFFS.getDescription(),
                    countryShardName, start.elapsedSince().asMilliseconds());
            return new Tuple2<>(tuple._1(), diffsList);
        };
    }

    /**
     * @param sharding
     *            The sharding tree
     * @return a Spark {@link PairFunction} that processes a shard to Atlas tuple, and constructs a
     *         {@link AtlasStatistics} for each shard.
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, AtlasStatistics> generateAtlasStatistics(
            final Broadcast<Sharding> sharding)
    {
        return tuple ->
        {
            final String countryShardName = getCountryShard(tuple._1()).getName();
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.SHARD_STATISTICS.getDescription(),
                    countryShardName);
            final Time start = Time.now();
            final Counter counter = new Counter().withSharding(sharding.getValue());
            counter.setCountsDefinition(Counter.POI_COUNTS_DEFINITION.getDefault());
            AtlasStatistics statistics = new AtlasStatistics();
            try
            {
                statistics = counter.processAtlas(tuple._2());
            }
            catch (final Exception e)
            {
                logger.error(ERROR_MESSAGE,
                        AtlasGeneratorJobGroup.SHARD_STATISTICS.getDescription(), countryShardName,
                        e);
            }
            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.SHARD_STATISTICS.getDescription(),
                    countryShardName, start.elapsedSince().asMilliseconds());
            return new Tuple2<>(tuple._1(), statistics);
        };
    }

    /**
     * This function takes a given tuple of shard to atlas and returns a set of tuples that are all
     * geometric relations mapped to their member geometries
     *
     * @param tuple
     *            the String to Atlas tuple to generate relation data for
     * @return iterator through the pairs described above
     */
    protected static Iterator<Tuple2<Long, Map<Ring, Iterable<PolyLine>>>> generateGeometricRelations(
            final Tuple2<String, Atlas> tuple)
    {
        final Atlas atlas = tuple._2;
        return StreamSupport
                .stream(atlas.relations(relation -> relation.isGeometric()).spliterator(), true)
                .map(relation ->
                {
                    final Map<Ring, Iterable<PolyLine>> outersInnersMap = new EnumMap<>(
                            Relation.Ring.class);
                    final List<PolyLine> outers = new ArrayList<>();
                    final List<PolyLine> inners = new ArrayList<>();
                    relation.members().forEach(member ->
                    {
                        PolyLine memberPolyLine = null;
                        if (member.getEntity().getType().equals(ItemType.LINE))
                        {
                            memberPolyLine = atlas.line(member.getEntity().getIdentifier())
                                    .asPolyLine();
                        }
                        else if (member.getEntity().getType().equals(ItemType.EDGE))
                        {
                            memberPolyLine = atlas.edge(member.getEntity().getIdentifier())
                                    .asPolyLine();
                        }
                        else if (member.getEntity().getType().equals(ItemType.AREA))
                        {
                            memberPolyLine = atlas.area(member.getEntity().getIdentifier())
                                    .asPolygon();
                        }
                        if (memberPolyLine != null)
                        {
                            if (member.getRole().equalsIgnoreCase(Ring.OUTER.toString()))
                            {
                                outers.add(memberPolyLine);
                            }
                            else if (member.getRole().equalsIgnoreCase(Ring.INNER.toString()))
                            {
                                inners.add(memberPolyLine);
                            }
                        }
                        outersInnersMap.put(Ring.INNER, inners);
                        outersInnersMap.put(Ring.OUTER, outers);
                    });
                    return new Tuple2<Long, Map<Ring, Iterable<PolyLine>>>(relation.getIdentifier(),
                            outersInnersMap);
                }).collect(Collectors.toList()).iterator();
    }

    /**
     * @param boundaries
     *            The {@link CountryBoundaryMap} to use for pbf to atlas generation
     * @param sparkContext
     *            Spark context (or configuration) as a key-value map
     * @param loadingOptions
     *            The basic required properties to create an {@link AtlasLoadingOption}
     * @param pbfContext
     *            The context explaining where to find the PBFs
     * @param atlasScheme
     *            The folder structure of the output atlas
     * @return a Spark {@link PairFunction} that processes an {@link AtlasGenerationTask}, loads the
     *         PBF for the task's shard, generates the raw atlas for the shard and outputs a shard
     *         name to raw atlas tuple.
     */
    protected static PairFunction<AtlasGenerationTask, String, Atlas> generateRawAtlas(
            final Broadcast<CountryBoundaryMap> boundaries, final Map<String, String> sparkContext,
            final Broadcast<Map<String, String>> loadingOptions, final PbfContext pbfContext,
            final SlippyTilePersistenceScheme atlasScheme)
    {
        return task ->
        {
            final String countryName = task.getCountry();
            final Shard shard = task.getShard();
            final String name = countryName + Shard.SHARD_DATA_SEPARATOR + shard.getName();
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.RAW.getDescription(), name);
            final Time start = Time.now();

            // Set the country code that is being processed!
            final AtlasLoadingOption atlasLoadingOption = AtlasGeneratorParameters
                    .buildAtlasLoadingOption(boundaries.getValue(), loadingOptions.getValue());
            atlasLoadingOption.setCountryCode(countryName);

            // Build the PbfLoader and generate the raw Atlas for this shard
            final Atlas atlas;
            try (PbfLoader loader = new PbfLoader(pbfContext, sparkContext, boundaries.getValue(),
                    atlasLoadingOption,
                    loadingOptions.getValue().get(AtlasGeneratorParameters.CODE_VERSION.getName()),
                    loadingOptions.getValue().get(AtlasGeneratorParameters.DATA_VERSION.getName()),
                    task.getAllShards()))
            {
                atlas = loader.generateRawAtlas(countryName, shard);
            }
            catch (final Throwable e) // NOSONAR
            {
                throw new CoreException(ERROR_MESSAGE, AtlasGeneratorJobGroup.RAW.getDescription(),
                        name, e);
            }

            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.RAW.getDescription(), name,
                    start.elapsedSince().asMilliseconds());

            // Output the Name/Atlas couple
            final String persistenceKey = PersistenceJsonParser.createJsonKey(countryName,
                    shard.getName(), atlasScheme.getScheme());
            return new Tuple2<>(persistenceKey, atlas);
        };
    }

    protected static PairFunction<Tuple2<String, AtlasStatistics>, String, NamedAtlasStatistics> groupAtlasStatisticsByCountry()
    {
        return tuple ->
        {
            final CountryShard countryShardName = getCountryShard(tuple._1());
            final String countryName = countryShardName.getCountry();
            return new Tuple2<>(
                    // Create the same key for all shards in the same country
                    PersistenceJsonParser.createJsonKey(countryName, "N/A", Maps.hashMap()),
                    // Here using NamedAtlasStatistics so the reduceByKey function below can
                    // name what statistic merging failed, if any.
                    new NamedAtlasStatistics(countryName, tuple._2()));
        };
    }

    /**
     * This function handles merging the relation member sets from different shards
     *
     * @return function that does the above
     */
    protected static Function2<Map<Ring, Iterable<PolyLine>>, Map<Ring, Iterable<PolyLine>>, Map<Ring, Iterable<PolyLine>>> mergeRelationMembers()
    {
        return (Serializable & Function2<Map<Ring, Iterable<PolyLine>>, Map<Ring, Iterable<PolyLine>>, Map<Ring, Iterable<PolyLine>>>) (
                a, b) ->
        {
            final Map<Ring, Iterable<PolyLine>> merged = new EnumMap<>(Ring.class);
            final Set<PolyLine> inners = new HashSet<>();
            final Set<PolyLine> outers = new HashSet<>();
            a.get(Ring.INNER).forEach(inners::add);
            b.get(Ring.INNER).forEach(inners::add);
            a.get(Ring.OUTER).forEach(outers::add);
            b.get(Ring.OUTER).forEach(outers::add);
            merged.put(Ring.OUTER, outers);
            merged.put(Ring.INNER, inners);
            return merged;
        };
    }

    protected static Function2<NamedAtlasStatistics, NamedAtlasStatistics, NamedAtlasStatistics> reduceAtlasStatistics()
    {
        return (left, right) ->
        {
            try
            {
                return new NamedAtlasStatistics(left.getName(), AtlasStatistics
                        .merge(left.getAtlasStatistics(), right.getAtlasStatistics()));
            }
            catch (final Throwable e) // NOSONAR
            {
                logger.error(
                        "Unable to merge AtlasStatistics for {}! Returning the first one only.\nLeft:\n{}\nRight:\n{}",
                        left.getName(), left.getAtlasStatistics(), right.getAtlasStatistics(), e);
                return left;
            }
        };
    }

    /**
     * @param boundaries
     *            The {@link CountryBoundaryMap} required to create an {@link AtlasLoadingOption}
     * @param sharding
     *            The {@link Sharding} strategy
     * @param sparkContext
     *            Spark context (or configuration) as a key-value map
     * @param loadingOptions
     *            The basic required properties to create an {@link AtlasLoadingOption}
     * @param edgeSubAtlasPath
     *            The path where the edge-only sub atlas files were saved
     * @param slicedAtlasPath
     *            The path where the sliced raw atlas files were saved
     * @param atlasScheme
     *            The folder structure of the output atlas
     * @return a Spark {@link PairFunction} that processes a tuple of shard-name and sliced raw
     *         atlas, sections the sliced raw atlas and returns the final sectioned (and sliced) raw
     *         atlas for that shard name.
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sectionAtlas(
            final Broadcast<CountryBoundaryMap> boundaries, final Broadcast<Sharding> sharding,
            final Map<String, String> sparkContext,
            final Broadcast<Map<String, String>> loadingOptions, final String edgeSubAtlasPath,
            final String slicedAtlasPath, final SlippyTilePersistenceScheme atlasScheme)
    {
        return tuple ->
        {
            final CountryShard countryShard = getCountryShard(tuple._1());
            final String countryShardName = countryShard.getName();

            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.WAY_SECTIONED_PBF.getDescription(),
                    countryShardName);
            final Time start = Time.now();
            final AtlasLoadingOption atlasLoadingOption = AtlasGeneratorParameters
                    .buildAtlasLoadingOption(boundaries.getValue(), loadingOptions.getValue());
            atlasLoadingOption.setCountryCode(countryShard.getCountry());
            // Instantiate the caches
            final HadoopAtlasFileCache atlasCache = new HadoopAtlasFileCache(slicedAtlasPath,
                    atlasScheme, sparkContext);
            final HadoopAtlasFileCache edgeSubCache = new HadoopAtlasFileCache(edgeSubAtlasPath,
                    atlasScheme, sparkContext);
            // Create the fetcher
            final Function<Shard, Optional<Atlas>> slicedRawAtlasFetcher = AtlasGeneratorHelper
                    .atlasFetcher(edgeSubCache, atlasCache, countryShard.getCountry(),
                            countryShard.getShard());

            final Atlas atlas;
            try
            {
                // Section the Atlas
                atlas = new AtlasSectionProcessor(countryShard.getShard(), atlasLoadingOption,
                        sharding.getValue(), slicedRawAtlasFetcher).run();
            }
            catch (final Throwable e) // NOSONAR
            {
                logger.warn("Sectioning had error, probably was null atlas",
                        new CoreException(ERROR_MESSAGE,
                                AtlasGeneratorJobGroup.WAY_SECTIONED_PBF.getDescription(),
                                countryShardName, e));
                return new Tuple2<String, Atlas>(tuple._1(), null);
            }

            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.WAY_SECTIONED_PBF.getDescription(),
                    countryShardName, start.elapsedSince().asMilliseconds());

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), atlas);
        };
    }

    /**
     * This function takes a given tuple of relation id to multipolygon and searches the shard map
     * for all atlases that relate to it. this is used later to compute which atlases need the
     * geometry assigned to them
     *
     * @param sharding
     *            the sharding tree
     * @param boundaries
     *            the CountryBoundaryMap
     * @param atlasScheme
     *            the scheme used to save the atlases
     * @return function that does the above
     */
    protected static PairFlatMapFunction<Tuple2<Long, MultiPolygon>, String, Set<Tuple2<Long, MultiPolygon>>> shardToRelationMap(
            final Broadcast<Sharding> sharding, final Broadcast<CountryBoundaryMap> boundaries,
            final SlippyTilePersistenceScheme atlasScheme)
    {
        return (Serializable & PairFlatMapFunction<Tuple2<Long, MultiPolygon>, String, Set<Tuple2<Long, MultiPolygon>>>) pair ->
        {
            final MultiPolygon geometry = pair._2;
            final Set<Shard> shards = new HashSet<>();
            final Geometry bounds = JtsPrecisionManager.getGeometryFactory()
                    .toGeometry(geometry.getEnvelopeInternal());
            if (bounds instanceof Polygon)
            {
                sharding.getValue()
                        .shards(new JtsPolygonConverter().backwardConvert((Polygon) bounds))
                        .forEach(shards::add);
            }
            final List<Tuple2<String, Set<Tuple2<Long, MultiPolygon>>>> shardToRelations = new ArrayList<>();
            shards.forEach(shard ->
            {

                for (final String country : boundaries.getValue()
                        .countryCodesOverlappingWith(shard.bounds()))
                {
                    final Set<Tuple2<Long, MultiPolygon>> relations = new HashSet<>();
                    relations.add(pair);
                    final String persistenceKey = PersistenceJsonParser.createJsonKey(country,
                            shard.getName(), atlasScheme.getScheme());
                    shardToRelations.add(new Tuple2<>(persistenceKey, relations));
                }
            });
            return shardToRelations.iterator();
        };
    }

    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sliceAtlas(
            final Broadcast<CountryBoundaryMap> boundaries,
            final Broadcast<Map<String, String>> loadingOptions)
    {
        return tuple ->
        {
            // Grab the tuple contents
            final CountryShard countryShard = getCountryShard(tuple._1());
            final String countryShardName = countryShard.getName();
            final Atlas rawAtlas = tuple._2();
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.SLICED.getDescription(),
                    countryShardName);
            final Time start = Time.now();

            final Atlas slicedAtlas;
            try
            {
                // Set the country code that is being processed!
                final AtlasLoadingOption atlasLoadingOption = AtlasGeneratorParameters
                        .buildAtlasLoadingOption(boundaries.getValue(), loadingOptions.getValue());
                atlasLoadingOption.setCountryCode(countryShard.getCountry());
                slicedAtlas = new RawAtlasSlicer(atlasLoadingOption, rawAtlas,
                        countryShard.getShard()).slice().cloneToPackedAtlas();
            }

            catch (final Throwable e) // NOSONAR
            {
                throw new CoreException(ERROR_MESSAGE,
                        AtlasGeneratorJobGroup.SLICED.getDescription(), countryShardName, e);
            }

            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.SLICED.getDescription(),
                    countryShardName, start.elapsedSince().asMilliseconds());

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), slicedAtlas);
        };
    }

    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> subatlas(
            final ConfiguredFilter filter, final AtlasCutType cutType)
    {
        return (Serializable & PairFunction<Tuple2<String, Atlas>, String, Atlas>) tuple ->
        {
            final Atlas subAtlas;
            // Grab the tuple contents
            final String countryShardName = getCountryShard(tuple._1()).getName();
            final Atlas originalAtlas = tuple._2();
            logger.info("Starting sub Atlas for for Atlas {}", originalAtlas.getName());
            final Time start = Time.now();
            try
            {
                // Slice the Atlas
                final Optional<Atlas> subAtlasOptional = originalAtlas.subAtlas(filter::test,
                        cutType);
                if (subAtlasOptional.isPresent())
                {
                    subAtlas = subAtlasOptional.get();
                }
                else
                {
                    subAtlas = null;
                    logger.error("Unable to extract valid subAtlas code for {}", countryShardName);
                }
            }
            catch (final Exception e) // NOSONAR
            {
                throw new CoreException("Sub Atlas failed for {}", countryShardName, e);
            }
            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), subAtlas);
        };
    }

    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> subatlas(
            final Predicate<AtlasEntity> filter, final AtlasCutType cutType)
    {
        return (Serializable & PairFunction<Tuple2<String, Atlas>, String, Atlas>) tuple ->
        {
            final Atlas subAtlas;

            // Grab the tuple contents
            final String countryShardName = getCountryShard(tuple._1()).getName();
            final Atlas originalAtlas = tuple._2();
            logger.info("Starting sub Atlas for for Atlas {}", originalAtlas.getName());
            final Time start = Time.now();

            try
            {
                // Slice the Atlas
                final Optional<Atlas> subAtlasOptional = originalAtlas.subAtlas(filter, cutType);
                if (subAtlasOptional.isPresent())
                {
                    subAtlas = subAtlasOptional.get();
                }
                else
                {
                    subAtlas = null;
                    logger.error("Unable to extract valid subAtlas code for {}", countryShardName);
                }

            }

            catch (final Exception e) // NOSONAR
            {
                throw new CoreException("Sub Atlas failed for {}", countryShardName, e);
            }

            logger.info("Finished sub Atlas for {} in {}", countryShardName, start.elapsedSince());

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), subAtlas);
        };
    }

    private static CountryShard getCountryShard(final String jsonKey)
    {
        return new CountryShard(PersistenceJsonParser.parseCountry(jsonKey),
                PersistenceJsonParser.parseShard(jsonKey));
    }

    /**
     * Hide constructor for this utility class.
     */
    private AtlasGeneratorHelper()
    {
    }
}
