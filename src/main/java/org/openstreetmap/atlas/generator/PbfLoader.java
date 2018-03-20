package org.openstreetmap.atlas.generator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.generator.PbfLocator.LocatedPbf;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasMetaData;
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.pbf.OsmPbfLoader;
import org.openstreetmap.atlas.geography.boundary.CountryBoundary;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.clipping.Clip.ClipType;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
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
public class PbfLoader implements Serializable
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
     *            The country boundary map
     * @param atlasLoadingOption
     *            The loading options for the {@link OsmPbfLoader}
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

    /**
     * Generate the {@link Atlas} for a {@link Shard}.
     *
     * @param countryName
     *            The Country to process
     * @param shard
     *            The shard to output
     * @return The build {@link Atlas} for the specified {@link Shard}. null if there is no Atlas to
     *         be build (because no PBF or empty PBFs or no overlap)
     */
    public Atlas load(final String countryName, final Shard shard)
    {
        final List<CountryBoundary> countryBoundaries = this.boundaries
                .countryBoundary(countryName);
        MultiPolygon boundary = null;
        for (final CountryBoundary countryBoundary : countryBoundaries)
        {
            if (countryBoundary.covers(shard.bounds()))
            {
                boundary = countryBoundary.getBoundary();
                break;
            }
        }
        if (boundary != null)
        {
            final MultiPolygon loadingArea = shard.bounds().clip(boundary, ClipType.AND)
                    .getClipMultiPolygon();
            final Iterable<LocatedPbf> pbfPool = this.locator.pbfsCovering(loadingArea);
            return loadFromPool(pbfPool, loadingArea, countryName, shard);
        }
        else
        {
            logger.error("Can't found shard {} for country {}", shard, countryName);
            return null;
        }
    }

    private Atlas loadFromPool(final Iterable<LocatedPbf> pbfs, final MultiPolygon loadingArea,
            final String country, final Shard shard)
    {
        final List<Atlas> atlases = new ArrayList<>();
        final Map<String, String> metaDataTags = Maps.hashMap();

        // Add shard information to the meta data
        metaDataTags.put("countryShards",
                this.countryShards.stream().map(countryShard -> country
                        + CountryShard.COUNTRY_SHARD_SEPARATOR + countryShard.getName())
                        .collect(Collectors.joining(",")));
        metaDataTags.put(shard.getName() + "_boundary", loadingArea.toString());

        pbfs.forEach(locatedPbf ->
        {
            final MultiPolygon pbfLoadingArea = locatedPbf.bounds().clip(loadingArea, ClipType.AND)
                    .getClipMultiPolygon();
            final AtlasMetaData metaData = new AtlasMetaData(null, true, this.codeVersion,
                    this.dataVersion, country, shard.getName(), metaDataTags);
            final OsmPbfLoader loader = new OsmPbfLoader(locatedPbf.getResource(), pbfLoadingArea,
                    this.atlasLoadingOption).withMetaData(metaData);
            Atlas shardPbfSlice = null;
            try
            {
                shardPbfSlice = loader.read();
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
        });
        if (ATLAS_SAVE_FOLDER != null)
        {
            int index = 0;
            for (final Atlas atlas : atlases)
            {
                atlas.save(ATLAS_SAVE_FOLDER.child(shard.getName() + "_" + index++ + ".atlas.gz"));
            }
        }
        if (atlases.size() > 1)
        {
            // Concatenate many PBFs in one single Atlas
            logger.info("Concatenating {} PBF-made Atlas into one Atlas Shard {}", atlases.size(),
                    shard);
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
}
