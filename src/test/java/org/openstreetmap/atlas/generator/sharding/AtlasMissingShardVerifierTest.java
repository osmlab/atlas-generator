package org.openstreetmap.atlas.generator.sharding;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.utilities.collections.StringList;

/**
 * Test for AtlasMissingShardVerifier.
 * 
 * @author jamesgage
 */
public class AtlasMissingShardVerifierTest
{
    @Test
    public void testCreateQueryList()
    {
        final CountryBoundaryMap boundaryMap = new CountryBoundaryMap();
        final GeometryFactory geoFactory = new GeometryFactory();
        final Coordinate point1 = new Coordinate(-64.544677734375, 35.3759765625);
        final Coordinate point2 = new Coordinate(71.158447265625, 50.8447265625);
        final Coordinate point3 = new Coordinate(71.158447265625, -27.9052734375);
        final Coordinate point4 = new Coordinate(-60.325927734375, -31.4208984375);
        final Coordinate[] coordinates = { point1, point2, point3, point4, point1 };
        final Polygon boundary = geoFactory.createPolygon(coordinates);
        boundaryMap.addCountry("WWW", boundary);
        final Set<CountryShard> countryShards = new HashSet<>();
        countryShards.add(new CountryShard("WWW", "1-0-0"));
        countryShards.add(new CountryShard("WWW", "1-1-0"));
        countryShards.add(new CountryShard("WWW", "1-0-1"));
        countryShards.add(new CountryShard("WWW", "1-1-1"));
        final StringList queries = AtlasMissingShardVerifier.createQueryList(boundaryMap,
                countryShards, 3);
        System.out.println(queries.get(0));
        System.out.println(queries.get(1));
        System.out.println(queries.get(2));
        System.out.println(queries.size());
        Assert.assertEquals("(node(0.0,-64.5446777,42.733401,0.0);<;);out body;", queries.get(0));
        Assert.assertEquals("(node(0.0,0.0,50.8447266,71.1584473);<;);out body;", queries.get(1));
        Assert.assertEquals(3, queries.size());
    }
}
