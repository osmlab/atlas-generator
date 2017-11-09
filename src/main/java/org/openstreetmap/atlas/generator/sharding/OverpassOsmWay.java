package org.openstreetmap.atlas.generator.sharding;

import java.util.HashMap;
import java.util.List;

/**
 * This way represents an osm way returned from an overpass query. The way identifier, the list of
 * node identifiers that comprise the way, and the tags are stored.
 *
 * @author jamesgage
 */
public class OverpassOsmWay
{
    private String osmIdentifier;
    private List<String> nodeIdentifiers;
    private HashMap<String, String> tags = new HashMap<>();

    public OverpassOsmWay(final String osmIdentifier, final List<String> nodeIdentifiers,
            final HashMap<String, String> tags)
    {
        this.osmIdentifier = osmIdentifier;
        this.nodeIdentifiers = nodeIdentifiers;
        this.tags = tags;
    }

    public List<String> getNodeIdentifiers()
    {
        return this.nodeIdentifiers;
    }

    public String getOsmIdentifier()
    {
        return this.osmIdentifier;
    }

    public HashMap<String, String> getTags()
    {
        return this.tags;
    }

    public void setNodeIdentifiers(final List<String> nodeIdentifiers)
    {
        this.nodeIdentifiers = nodeIdentifiers;
    }

    public void setOsmIdentifier(final String osmIdentifier)
    {
        this.osmIdentifier = osmIdentifier;
    }

    public void setTags(final HashMap<String, String> tags)
    {
        this.tags = tags;
    }

    @Override
    public String toString()
    {
        return "ID: " + this.osmIdentifier + "\nNodes: " + this.nodeIdentifiers.toString()
                + "\nTags: " + this.tags.toString() + "\n";
    }

}
