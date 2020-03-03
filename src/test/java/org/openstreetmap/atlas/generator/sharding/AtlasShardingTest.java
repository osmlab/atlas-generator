package org.openstreetmap.atlas.generator.sharding;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.sharding.GeoHashSharding;
import org.openstreetmap.atlas.geography.sharding.Sharding;

/**
 * @author matthieun
 */
public class AtlasShardingTest
{
    @Test(expected = CoreException.class)
    public void testDynamicSharding()
    {
        AtlasSharding.forString("dynamic@resource://some/file/that/does/not/exist",
                ResourceFileSystem.simpleconfiguration());
    }

    @Test
    public void testGeoHashSharding()
    {
        final Sharding sharding = AtlasSharding.forString("geohash@7",
                ResourceFileSystem.simpleconfiguration());
        Assert.assertTrue(sharding instanceof GeoHashSharding);
        final GeoHashSharding geoHashSharding = (GeoHashSharding) sharding;
        Assert.assertEquals(7, geoHashSharding.getPrecision());
    }
}
