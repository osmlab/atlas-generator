package org.openstreetmap.atlas.generator.sharding;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.writers.SafeBufferedWriter;
import org.openstreetmap.atlas.tags.Taggable;
import org.openstreetmap.atlas.tags.filters.ConfiguredTaggableFilter;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks intersection of country boundary and shards to validate missing shards.
 *
 * @author james_gage
 */
public class AtlasMissingShardVerifier extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(AtlasMissingShardVerifier.class);

    private static final Switch<CountryBoundaryMap> BOUNDARIES = new Switch<>("boundaries",
            "The country boundaries.", value -> initializeCountryBoundaryMap(value),
            Optionality.REQUIRED);
    private static final Switch<File> OUTPUT = new Switch<>("output", "The output file", File::new,
            Optionality.REQUIRED);
    private static final Switch<File> MISSING_SHARDS = new Switch<>("shards",
            "Country shards that are listed as missing", File::new, Optionality.REQUIRED);
    private static final Switch<String> OVERPASS_SERVER = new Switch<>("server",
            "The overpass server to query from", value -> value, Optionality.OPTIONAL);

    public static void main(final String[] args)
    {
        new AtlasMissingShardVerifier().run(args);
    }

    private static CountryBoundaryMap initializeCountryBoundaryMap(final String value)
    {
        final Time start = Time.now();
        logger.info("Loading boundaries");
        final CountryBoundaryMap result = new CountryBoundaryMap(new File(value));
        logger.info("Loaded boundaries in {}", start.elapsedSince());
        return result;
    }

    public int verifier(final CountryBoundaryMap boundaries,
            final Set<CountryShard> missingCountryShards, final File output, final String server)
    {
        int returnCode = 0;
        try (SafeBufferedWriter writer = output.writer())
        {
            final OverpassClient client = new OverpassClient(server);
            final List<CountryShard> verifiedMissingShards = new ArrayList<>();
            for (final CountryShard countryShard : missingCountryShards)
            {
                logger.info(countryShard.toString());
                final Clip clip = new Clip(ClipType.AND, countryShard.bounds(),
                        boundaries.countryBoundary(countryShard.getCountry()).get(0).getBoundary());
                if (clip.getClipMultiPolygon().surface().asDm7Squared() == 0)
                {
                    continue;
                }
                final MultiPolygon clipMulti = clip.getClipMultiPolygon();
                final Rectangle clipBounds = clipMulti.bounds();
                // Take in list of all nodes, then filter out nodes that aren't ingested into
                // Atlas
                final Time nodeStart = Time.now();
                final List<OverpassOsmNode> nodeList = client
                        .nodesFromQuery(OverpassClient.buildQuery("node", clipBounds));
                logger.info("Node query finished in: " + nodeStart.elapsedSince());
                final Time wayStart = Time.now();
                final List<OverpassOsmWay> ways = client
                        .waysFromQuery(OverpassClient.buildQuery("way", clipBounds));
                logger.info("Way query finished in: " + wayStart.elapsedSince());
                final Resource resource = new InputStreamResource(
                        AtlasMissingShardVerifier.class.getResourceAsStream(
                                "/org/openstreetmap/atlas/geography/atlas/pbf/osm-pbf-way.json"));
                final Configuration configuration = new StandardConfiguration(resource);
                final ConfiguredTaggableFilter filter = new ConfiguredTaggableFilter(configuration);
                for (final OverpassOsmWay way : ways)
                {
                    final Taggable taggableWay = Taggable.with(way.getTags());
                    if (!filter.test(taggableWay))
                    {
                        nodeList.removeIf(
                                node -> way.getNodeIdentifiers().contains(node.getIdentifier()));
                    }
                }
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
                        writer.writeLine("Id of relevant node geometry: " + node.getIdentifier());
                        writer.writeLine("Node Location: " + nodeLocation.toString() + "\n");
                        verifiedMissingShards.add(countryShard);
                        break;
                    }
                }
            }
            if (verifiedMissingShards.isEmpty())
            {
                logger.info("No shards are missing!");
            }
            else
            {
                verifiedMissingShards
                        .forEach(value -> logger.info(value.toString() + " is missing!"));
            }
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not match shards to boundaries", e);
        }
        return returnCode;
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final CountryBoundaryMap boundaries = (CountryBoundaryMap) command.get(BOUNDARIES);
        final File missingShardFile = (File) command.get(MISSING_SHARDS);
        final File output = (File) command.get(OUTPUT);
        final Set<CountryShard> missingCountryShards = missingShardFile.linesList().stream()
                .map(CountryShard::forName).collect(Collectors.toSet());
        final String server = (String) command.get(OVERPASS_SERVER);
        final Time fullStart = Time.now();
        final int returnCode;
        try
        {
            returnCode = verifier(boundaries, missingCountryShards, output, server);
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
        return new SwitchList().with(BOUNDARIES, OUTPUT, MISSING_SHARDS, OVERPASS_SERVER);
    }
}
