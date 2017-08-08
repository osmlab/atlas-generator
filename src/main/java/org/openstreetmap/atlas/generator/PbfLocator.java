package org.openstreetmap.atlas.generator;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.geography.Located;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locate the right PBF {@link Resource}s from a store
 *
 * @author matthieun
 */
public class PbfLocator implements Serializable
{
    /**
     * @author matthieun
     */
    public static class LocatedPbf implements Located, Serializable
    {
        private static final long serialVersionUID = 1033855164646532750L;

        private final Resource resource;
        private final Rectangle bounds;

        public LocatedPbf(final Resource resource, final Rectangle bounds)
        {
            this.resource = resource;
            this.bounds = bounds;
        }

        @Override
        public Rectangle bounds()
        {
            return this.bounds;
        }

        public Resource getResource()
        {
            return this.resource;
        }
    }

    private static final long serialVersionUID = -5831212599367503519L;

    private static final Logger logger = LoggerFactory.getLogger(PbfLocator.class);

    private final PbfContext pbfContext;
    private final Map<String, String> sparkContext;
    private final Function<SlippyTile, LocatedPbf> tileToLocatedPbf;

    /**
     * Construct
     *
     * @param pbfContext
     *            The context for the PBF input
     * @param spark
     *            The spark context that will help connect to the data source
     */
    public PbfLocator(final PbfContext pbfContext, final Map<String, String> spark)
    {
        this.pbfContext = pbfContext;
        this.sparkContext = spark;
        this.tileToLocatedPbf = (Function<SlippyTile, LocatedPbf> & Serializable) tile ->
        {
            final Path pbf = new Path(this.pbfContext.getPbfPath() + "/" + tile.getZoom() + "-"
                    + tile.getX() + "-" + tile.getY() + ".pbf");
            logger.info("Locating PBF for Tile {} at {}", tile, pbf.toString());
            final FileSystem fileSystem = new FileSystemCreator().get(this.pbfContext.getPbfPath(),
                    this.sparkContext);
            try
            {
                if (!fileSystem.exists(pbf))
                {
                    logger.warn("PBF Resource {} does not exist.", pbf.toString());
                    return null;
                }
            }
            catch (final IOException e)
            {
                throw new CoreException("Cannot test if {} exists.", pbf.toString());
            }
            return new LocatedPbf(new InputStreamResource(() ->
            {
                try
                {
                    return fileSystem.open(pbf);
                }
                catch (final Exception e)
                {
                    throw new CoreException("Cannot translate {} to a PBF resource.", tile, e);
                }
            }).withName(pbf.toString()), tile.bounds());
        };
    }

    /**
     * @param multiPolygon
     *            The {@link MultiPolygon} parameter
     * @return All the PBF resources covering the provided {@link MultiPolygon}
     */
    public Iterable<LocatedPbf> pbfsCovering(final MultiPolygon multiPolygon)
    {
        logger.trace("Seeking tiles for MultiPolygon {}", multiPolygon.toSimpleString());
        final List<Polygon> inners = multiPolygon.inners();
        final Iterable<Shard> tileIterable = Iterables.stream(multiPolygon.outers())
                .flatMap(PbfLocator.this::tilesCoveringPartially);
        final Set<Shard> tileSet = Iterables.asSet(tileIterable);
        logger.trace("Found tiles {} for MultiPolygon {}", tileSet, multiPolygon.toSimpleString());
        final Iterable<LocatedPbf> resourcesWithNulls = Iterables.stream(tileSet)
                .filter(tile -> !innerCovers(tile, inners)).map(shard -> (SlippyTile) shard)
                .map(this.tileToLocatedPbf);
        // Filter out all the null resources, meaning where the PBFs were not found. (Ocean for
        // example.)
        return Iterables.filter(resourcesWithNulls, resource -> resource != null);
    }

    /**
     * @param polygon
     *            The {@link Polygon} parameter
     * @return All the PBF resources covering the provided {@link Polygon}
     */
    public Iterable<LocatedPbf> pbfsCovering(final Polygon polygon)
    {
        logger.trace("Seeking tiles for Polygon {}", polygon.toSimpleString());
        return Iterables.stream(tilesCoveringPartially(polygon)).map(shard -> (SlippyTile) shard)
                .map(this.tileToLocatedPbf);
    }

    private boolean innerCovers(final Shard shard, final List<Polygon> inners)
    {
        for (final Polygon inner : inners)
        {
            if (inner.fullyGeometricallyEncloses(shard.bounds()))
            {
                return true;
            }
        }
        return false;
    }

    private Iterable<? extends Shard> tilesCoveringPartially(final Polygon polygon)
    {
        return this.pbfContext.getSharding().shards(polygon);
    }
}
