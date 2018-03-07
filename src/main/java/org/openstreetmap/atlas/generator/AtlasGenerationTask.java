package org.openstreetmap.atlas.generator;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.geography.sharding.Shard;

/**
 * Class to encapsulate required artifacts, information for Atlas generation process.
 *
 * @author mkalender
 */
final class AtlasGenerationTask implements Serializable
{
    private static final long serialVersionUID = -3730018742291904849L;

    private final String country;
    private final Shard shard;
    private final List<Shard> allShards;

    /**
     * Default constructor
     *
     * @param country
     *            Country name that Atlas generation will be executed for
     * @param shard
     *            {@link Shard} that Atlas generation will be executed for
     * @param allShards
     *            All {@link Shard}s for country
     */
    AtlasGenerationTask(final String country, final Shard shard, final List<Shard> allShards)
    {
        this.country = country;
        this.shard = shard;
        this.allShards = allShards;
    }

    List<Shard> getAllShards()
    {
        return this.allShards;
    }

    String getCountry()
    {
        return this.country;
    }

    Shard getShard()
    {
        return this.shard;
    }

    @Override
    public String toString()
    {
        return String.format("%s - %s:%s%s", this.getCountry(), this.getShard(),
                System.lineSeparator(),
                this.getAllShards().stream().map(shard -> String.format(" - %s", shard))
                        .collect(Collectors.joining(System.lineSeparator())));
    }
}
