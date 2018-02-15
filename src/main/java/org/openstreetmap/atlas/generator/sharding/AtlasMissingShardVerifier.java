package org.openstreetmap.atlas.generator.sharding;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.clipping.Clip;
import org.openstreetmap.atlas.geography.clipping.Clip.ClipType;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.writers.SafeBufferedWriter;
import org.openstreetmap.atlas.tags.Taggable;
import org.openstreetmap.atlas.tags.filters.ConfiguredTaggableFilter;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

/**
 * Checks intersection of country boundary and shards to validate missing shards.
 *
 * @author jwpgage
 */
public class AtlasMissingShardVerifier extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(AtlasMissingShardVerifier.class);
    private static final String FILTER_CONFIG = "/org/openstreetmap/atlas/geography/atlas/pbf/osm-pbf-way.json";

    private static final Switch<CountryBoundaryMap> BOUNDARIES = new Switch<>("boundaries",
            "The country boundaries.", value -> initializeCountryBoundaryMap(value),
            Optionality.REQUIRED);
    private static final Switch<File> OUTPUT = new Switch<>("output", "The output file", File::new,
            Optionality.REQUIRED);
    private static final Switch<File> MISSING_SHARDS = new Switch<>("shards",
            "Country shards that are listed as missing", File::new, Optionality.REQUIRED);
    private static final Switch<String> OVERPASS_SERVER = new Switch<>("server",
            "The overpass server to query from", value -> value, Optionality.OPTIONAL);
    private static final Switch<StringList> PROXY_SETTINGS = new Switch<>("proxySettings",
            "Proxy host and port number, split by comma", value -> StringList.split(value, ","),
            Optionality.OPTIONAL);

    public static void main(final String[] args)
    {
        new AtlasMissingShardVerifier().run(args);
    }

    private static String createMasterQuery(final CountryBoundaryMap boundaries,
            final Set<CountryShard> missingCountryShards)
    {
        // Build Overpass query that downloads all the nodes and ways needed
        final StringBuilder masterQuery = new StringBuilder("(");
        for (final CountryShard countryShard : missingCountryShards)
        {
            final Clip clip = intersectionClip(countryShard, boundaries);
            final Rectangle clipBounds = clip.getClipMultiPolygon().bounds();
            masterQuery.append(OverpassClient.buildCompactQuery("node", clipBounds));
        }
        masterQuery.append(");out body;");
        return masterQuery.toString();
    }

    private static HttpHost createProxy(final StringList proxySettings) throws NumberFormatException
    {
        final String proxyHost = proxySettings.get(0);
        final int proxyPort = Integer.parseInt(proxySettings.get(1));
        logger.info("Proxy Host: " + proxyHost + " Port: " + proxyPort);
        return new HttpHost(proxyHost, proxyPort);
    }

    private static CountryBoundaryMap initializeCountryBoundaryMap(final String value)
    {
        final Time start = Time.now();
        logger.info("Loading boundaries");
        final CountryBoundaryMap result = new CountryBoundaryMap(new File(value));
        logger.info("Loaded boundaries in {}", start.elapsedSince());
        return result;
    }

    private static STRtree initializeNodeTree(final List<OverpassOsmNode> nodes)
    {
        final STRtree nodeTree = new STRtree();
        nodes.forEach(node ->
        {
            nodeTree.insert(new Envelope(new Coordinate(Double.parseDouble(node.getLongitude()),
                    Double.parseDouble(node.getLatitude()))), node);
        });
        logger.info("Imported " + nodeTree.size() + " nodes to tree");
        return nodeTree;
    }

    private static STRtree initializeWayTree(final List<OverpassOsmNode> nodes,
            final List<OverpassOsmWay> ways)
    {
        final STRtree wayTree = new STRtree();
        final HashMap<String, OverpassOsmNode> nodeIds = new HashMap<>();
        nodes.forEach(node -> nodeIds.put(node.getIdentifier(), node));
        ways.forEach(way ->
        {
            Envelope startPoint = new Envelope();
            for (final String nodeId : way.getNodeIdentifiers())
            {
                if (startPoint == null && nodeIds.containsKey(nodeId))
                {
                    startPoint = new Envelope(
                            new Coordinate(Double.parseDouble(nodeIds.get(nodeId).getLongitude()),
                                    Double.parseDouble(nodeIds.get(nodeId).getLatitude())));
                }
                else if (nodeIds.containsKey(nodeId))
                {
                    final Coordinate nextPoint = new Coordinate(
                            Double.parseDouble(nodeIds.get(nodeId).getLongitude()),
                            Double.parseDouble(nodeIds.get(nodeId).getLatitude()));
                    startPoint.expandToInclude(nextPoint);
                }
            }
            wayTree.insert(startPoint, way);
        });
        logger.info("Imported " + wayTree.size() + " ways to tree");
        return wayTree;
    }

    private static Clip intersectionClip(final CountryShard countryShard,
            final CountryBoundaryMap boundaries)
    {
        return new Clip(ClipType.AND, countryShard.bounds(),
                boundaries.countryBoundary(countryShard.getCountry()).get(0).getBoundary());
    }

    private static Set<CountryShard> removeShardsWithZeroIntersection(
            final Set<CountryShard> missingCountryShards, final CountryBoundaryMap boundaries)
    {
        missingCountryShards.removeIf(countryShard ->
        {
            final Clip clip = intersectionClip(countryShard, boundaries);
            return clip.getClipMultiPolygon().surface().asDm7Squared() == 0;
        });
        return missingCountryShards;
    }

    public int verifier(final CountryBoundaryMap boundaries,
            final Set<CountryShard> missingCountryShardsUntrimmed, final File output,
            final String server, final HttpHost proxy)
    {
        int returnCode = 0;
        final Set<CountryShard> missingCountryShards = removeShardsWithZeroIntersection(
                missingCountryShardsUntrimmed, boundaries);
        final String masterQuery = createMasterQuery(boundaries, missingCountryShards);
        final OverpassClient client = new OverpassClient(server, proxy);
        final ConfiguredTaggableFilter filter = new ConfiguredTaggableFilter(
                new StandardConfiguration(new InputStreamResource(
                        AtlasMissingShardVerifier.class.getResourceAsStream(FILTER_CONFIG))));
        try (SafeBufferedWriter writer = output.writer())
        {
            final List<OverpassOsmNode> nodes = client.nodesFromQuery(masterQuery);
            final List<OverpassOsmWay> ways = client.waysFromQuery(masterQuery);
            if (client.hasTooMuchResponseData())
            {
                throw new CoreException(
                        "The overpass query returned too much data. This means that there are large amounts of data missing! Check the missing shard list for outliers.");
            }
            if (client.hasUnknownError())
            {
                throw new CoreException(
                        "The overpass query encountered an error. Validation has failed.");
            }
            final STRtree nodeTree = initializeNodeTree(nodes);
            final STRtree wayTree = initializeWayTree(nodes, ways);
            for (final CountryShard countryShard : missingCountryShards)
            {
                final Clip clip = intersectionClip(countryShard, boundaries);
                final MultiPolygon clipMulti = clip.getClipMultiPolygon();
                final Rectangle clipBounds = clipMulti.bounds();
                @SuppressWarnings("unchecked")
                final List<OverpassOsmNode> nodeList = nodeTree.query(clipBounds.asEnvelope());
                // Prune extra nodes returned by STRtree that might not actually be contained within
                // clipBounds
                nodeList.removeIf(node ->
                {
                    return !clipBounds.fullyGeometricallyEncloses(
                            Location.forString(node.getLatitude() + "," + node.getLongitude()));
                });
                @SuppressWarnings("unchecked")
                final List<OverpassOsmWay> wayList = wayTree.query(clipBounds.asEnvelope());
                // Filter out ways that aren't ingested into atlas
                wayList.stream().filter(way -> !filter.test(Taggable.with(way.getTags())))
                        .forEach(way ->
                        {
                            nodeList.removeIf(node -> way.getNodeIdentifiers()
                                    .contains(node.getIdentifier()));
                        });
                // Loop over the remaining nodes, which now consist of only nodes that should be
                // ingested into atlas
                // If a node is found within the intersection of the country boundary and shard
                // bounds then the shard should have been built, so break and list the shard
                for (final OverpassOsmNode node : nodeList)
                {
                    final Location nodeLocation = Location
                            .forString(node.getLatitude() + "," + node.getLongitude());
                    if (clipMulti.fullyGeometricallyEncloses(nodeLocation))
                    {
                        returnCode = -1;
                        writer.writeLine(countryShard.toString());
                        writer.writeLine(
                                "Boundary/Shard intersection zone: " + clipMulti.toString());
                        writer.writeLine("Id of node that should have been imported: "
                                + node.getIdentifier());
                        writer.writeLine("Node Location: " + nodeLocation.toString() + "\n");
                        logger.info(countryShard.toString() + " is missing!");
                        break;
                    }
                }
            }
        }
        catch (final Exception e)
        {
            logger.error("Error!", e);
            return -1;
        }
        if (returnCode == 0)
        {
            logger.info("No shards are missing!");
        }
        return returnCode;
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final CountryBoundaryMap boundaries = (CountryBoundaryMap) command.get(BOUNDARIES);
        final File missingShardFile = (File) command.get(MISSING_SHARDS);
        final File output = (File) command.get(OUTPUT);
        if (!missingShardFile.exists() || missingShardFile.all().equals(""))
        {
            logger.info("No missing shards to check!");
            return 0;
        }
        final Set<CountryShard> missingCountryShards = missingShardFile.linesList().stream()
                .map(CountryShard::forName).collect(Collectors.toSet());
        final String server = (String) command.get(OVERPASS_SERVER);
        final StringList proxySettings = (StringList) command.get(PROXY_SETTINGS);
        HttpHost proxy = null;
        if (proxySettings != null)
        {
            try
            {
                proxy = createProxy(proxySettings);
            }
            catch (final Exception e)
            {
                logger.error("Proxy settings not correctly set", e);
                return -1;
            }
        }
        final Time fullStart = Time.now();
        final int returnCode;
        try
        {
            returnCode = verifier(boundaries, missingCountryShards, output, server, proxy);
        }
        catch (final Exception e)
        {
            return -1;
        }
        logger.info("Verification ran in: " + fullStart.elapsedSince());
        return returnCode;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(BOUNDARIES, OUTPUT, MISSING_SHARDS, OVERPASS_SERVER,
                PROXY_SETTINGS);
    }
}
