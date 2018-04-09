package org.openstreetmap.atlas.generator;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.geography.Located;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
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

    public static final String DEFAULT_SCHEME = SlippyTilePersistenceScheme.ZOOM + "-"
            + SlippyTilePersistenceScheme.X_INDEX + "-" + SlippyTilePersistenceScheme.Y_INDEX
            + FileSuffix.PBF.toString();

    private final PbfContext pbfContext;
    private final Function<SlippyTile, Optional<LocatedPbf>> pbfFetcher;

    /**
     * Construct
     *
     * @param pbfContext
     *            The pbf context
     * @param spark
     *            The spark context that will help connect to the data source
     */
    public PbfLocator(final PbfContext pbfContext, final Map<String, String> spark)
    {
        this.pbfContext = pbfContext;
        final FileSystem fileSystem = new FileSystemCreator().get(this.pbfContext.getPbfPath(),
                spark);
        this.pbfFetcher = (Function<SlippyTile, Optional<LocatedPbf>> & Serializable) shard ->
        {
            final Path pbfName = new Path(this.pbfContext.getPbfPath() + "/"
                    + this.pbfContext.getScheme().compile(shard));
            try
            {
                if (!fileSystem.exists(pbfName))
                {
                    logger.warn("PBF Resource {} does not exist.", pbfName.toString());
                    return Optional.empty();
                }
            }
            catch (final IOException e)
            {
                throw new CoreException("Cannot test if {} exists.", pbfName.toString());
            }
            final LocatedPbf locatedPbf = new LocatedPbf(new InputStreamResource(() ->
            {
                try
                {
                    return fileSystem.open(pbfName);
                }
                catch (final Exception e)
                {
                    throw new CoreException("Cannot translate {} to a PBF resource.", shard, e);
                }
            }).withName(pbfName.toString()), shard.bounds());
            return Optional.of(locatedPbf);
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
        // Filter out all the empty resources, meaning where the PBFs were not found. (Ocean for
        // example.)
        return Iterables.stream(tileSet).filter(tile -> !innerCovers(tile, inners))
                .map(shard -> (SlippyTile) shard).map(this.pbfFetcher).filter(Optional::isPresent)
                .map(Optional::get).collect();
    }

    /**
     * @param polygon
     *            The {@link Polygon} parameter
     * @return All the PBF resources covering the provided {@link Polygon}
     */
    public Iterable<LocatedPbf> pbfsCovering(final Polygon polygon)
    {
        logger.trace("Seeking tiles for Polygon {}", polygon.toSimpleString());
        // Filter out all the empty resources, meaning where the PBFs were not found. (Ocean for
        // example.)
        return Iterables.stream(tilesCoveringPartially(polygon)).map(shard -> (SlippyTile) shard)
                .map(this.pbfFetcher).filter(Optional::isPresent).map(Optional::get).collect();
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
