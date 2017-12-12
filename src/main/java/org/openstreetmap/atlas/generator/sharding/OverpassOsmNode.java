package org.openstreetmap.atlas.generator.sharding;

import java.util.HashMap;
import java.util.Map;

/**
 * This node represents an Osm Node returned from an overpass query. The node identifier, Location,
 * and tags are stored.
 *
 * @author jwpgage
 */
public class OverpassOsmNode
{
    private String osmIdentifier;
    private String latitude;
    private String longitude;
    private final Map<String, String> tags;

    public OverpassOsmNode(final String osmIdentifier, final String latitude,
            final String longitude)
    {
        this(osmIdentifier, latitude, longitude, new HashMap<>());
    }

    public OverpassOsmNode(final String osmIdentifier, final String latitude,
            final String longitude, final Map<String, String> tags)
    {
        this.osmIdentifier = osmIdentifier;
        this.latitude = latitude;
        this.longitude = longitude;
        this.tags = tags;
    }

    public String getIdentifier()
    {
        return this.osmIdentifier;
    }

    public String getLatitude()
    {
        return this.latitude;
    }

    public String getLongitude()
    {
        return this.longitude;
    }

    public Map<String, String> getTags()
    {
        return this.tags;
    }

    public void setIdentifier(final String osmIdentifier)
    {
        this.osmIdentifier = osmIdentifier;
    }

    public void setLatitude(final String latitude)
    {
        this.latitude = latitude;
    }

    public void setLongitude(final String longitude)
    {
        this.longitude = longitude;
    }

    @Override
    public String toString()
    {
        return "ID=" + this.osmIdentifier + " LAT=" + this.latitude + " LON=" + this.longitude;
    }

}
