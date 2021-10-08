package org.openstreetmap.atlas.generator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedPolygon;
import org.openstreetmap.atlas.generator.PbfLocator.LocatedPbf;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasMetaData;
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.raw.creation.RawAtlasGenerator;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.clipping.Clip.ClipType;
import org.openstreetmap.atlas.geography.converters.jts.JtsMultiPolygonConverter;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load a set of {@link Resource}s pbfs and build a single Atlas from it, following the wanted
 * country border and shard
 *
 * @author matthieun
 */
public class PbfLoader implements AutoCloseable, Serializable
{
    private static final long serialVersionUID = -3991330796225288845L;

    private static final Logger logger = LoggerFactory.getLogger(PbfLoader.class);

    // This is to debug atlas creation
    private static File ATLAS_SAVE_FOLDER;

    private final CountryBoundaryMap boundaries;
    private final PbfLocator locator;
    private final AtlasLoadingOption atlasLoadingOption;
    private final String codeVersion;
    private final String dataVersion;
    private final Set<Shard> countryShards;

    public static void setAtlasSaveFolder(final File atlasSaveFolder)
    {
        ATLAS_SAVE_FOLDER = atlasSaveFolder;
    }

    /**
     * Construct
     *
     * @param pbfContext
     *            The context explaining where to find the PBFs
     * @param sparkContext
     *            The context from Spark
     * @param boundaries
     *            The {@link CountryBoundaryMap}
     * @param atlasLoadingOption
     *            The loading options
     * @param codeVersion
     *            The version of the code used
     * @param dataVersion
     *            The version of the data in the PBFs
     * @param countryShards
     *            {@link Set} of {@link Shard}s for the Atlas meta data
     */
    public PbfLoader(final PbfContext pbfContext, final Map<String, String> sparkContext,
            final CountryBoundaryMap boundaries, final AtlasLoadingOption atlasLoadingOption,
            final String codeVersion, final String dataVersion, final Set<Shard> countryShards)
    {
        this.boundaries = boundaries;
        this.atlasLoadingOption = atlasLoadingOption;
        atlasLoadingOption.setCountryBoundaryMap(boundaries);
        this.locator = new PbfLocator(pbfContext, sparkContext);
        this.codeVersion = codeVersion;
        this.dataVersion = dataVersion;
        this.countryShards = countryShards;
    }

    @Override
    public void close() throws IOException
    {
        this.locator.close();
    }

    public Atlas generateRawAtlas(final String countryName, final Shard shard)
    {
        final Optional<MultiPolygon> loadingArea = calculateLoadingArea(countryName, shard);
        if (loadingArea.isPresent())
        {
            final Iterable<LocatedPbf> pbfPool = this.locator.pbfsCovering(loadingArea.get());

            final List<Atlas> atlases = new ArrayList<>();
            final Map<String, String> metaDataTags = Maps.hashMap();

            // Add shard information to the meta data
            metaDataTags.put("countryShards",
                    this.countryShards.stream().map(countryShard -> countryName
                            + Shard.SHARD_DATA_SEPARATOR + countryShard.getName())
                            .collect(Collectors.joining(",")));

            // For each PBF, create an atlas
            pbfPool.forEach(locatedPbf ->
            {
                final MultiPolygon pbfLoadingArea = locatedPbf.bounds()
                        .clip(loadingArea.get(), ClipType.AND).getClipMultiPolygon();

                // Guard against an empty loading area
                if (!pbfLoadingArea.isEmpty())
                {
                    final AtlasMetaData metaData = new AtlasMetaData(null, true, this.codeVersion,
                            this.dataVersion, countryName, shard.getName(), metaDataTags);
                    Atlas shardPbfSlice = null;
                    try
                    {
                        final RawAtlasGenerator rawAtlasGenerator = new RawAtlasGenerator(
                                locatedPbf.getResource(), this.atlasLoadingOption, pbfLoadingArea)
                                        .withMetaData(metaData);
                        // The keepAll option is primarily used for QA. Don't trim.
                        if (this.atlasLoadingOption.isKeepAll())
                        {
                            shardPbfSlice = rawAtlasGenerator.buildNoTrim();
                        }
                        else
                        {
                            shardPbfSlice = rawAtlasGenerator.build();
                        }
                    }
                    catch (final Exception e)
                    {
                        logger.error("Dropping PBF {} for Atlas shard {}",
                                locatedPbf.getResource().getName(), shard, e);
                    }
                    if (shardPbfSlice != null)
                    {
                        atlases.add(shardPbfSlice);
                    }
                }
            });

            // Save, if needed
            if (ATLAS_SAVE_FOLDER != null)
            {
                int index = 0;
                for (final Atlas atlas : atlases)
                {
                    atlas.save(
                            ATLAS_SAVE_FOLDER.child(shard.getName() + "_" + index++ + ".atlas.gz"));
                }
            }

            if (atlases.size() > 1)
            {
                // Concatenate many PBFs in one single Atlas
                logger.info("Concatenating {} PBF-made Atlas into one Atlas Shard {}",
                        atlases.size(), shard);
                return PackedAtlas.cloneFrom(new MultiAtlas(atlases));
            }
            else if (atlases.size() == 1)
            {
                // Only one PBF was used
                return atlases.get(0);
            }
            else
            {
                // There are no PBF resources.
                return null;
            }
        }
        else
        {
            return null;
        }
    }

    /**
     * Calculates the loading area given a country name and working {@link Shard}.
     *
     * @param countryName
     *            The country being built
     * @param shard
     *            The shard being processed
     * @return the intersection of given country's boundary clipped with the given shard's bounds
     */
    private Optional<MultiPolygon> calculateLoadingArea(final String countryName, final Shard shard)
    {
        final List<PreparedPolygon> countryBoundaries = this.boundaries
                .getCountryNameToBoundaryMap().get(countryName);
        final Set<Polygon> boundaryPolygons = new HashSet<>();
        for (final PreparedPolygon countryBoundary : countryBoundaries)
        {
            if (countryBoundary.intersects(countryBoundary.getGeometry().getFactory()
                    .toGeometry(shard.bounds().asEnvelope())))
            {
                boundaryPolygons.add((Polygon) countryBoundary.getGeometry());
            }
        }
        if (!boundaryPolygons.isEmpty())
        {
            final MultiPolygon boundary = new JtsMultiPolygonConverter()
                    .backwardConvert(boundaryPolygons);
            return Optional.of(shard.bounds().clip(boundary, ClipType.AND).getClipMultiPolygon());
        }
        else
        {
            logger.error("Can't find shard {} for country {}", shard, countryName);
            return Optional.empty();
        }
    }
}
