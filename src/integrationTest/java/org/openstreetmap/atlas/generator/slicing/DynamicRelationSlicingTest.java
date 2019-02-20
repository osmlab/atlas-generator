package org.openstreetmap.atlas.generator.slicing;

import java.util.HashMap;

import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.geography.atlas.pbf.slicing.DynamicRelationSlicingTestRule;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.streaming.resource.File;

/**
 * @author jamesgage
 */
public class DynamicRelationSlicingTest
{
    @Rule
    public DynamicRelationSlicingTestRule rule = new DynamicRelationSlicingTestRule();

    @Test
    public void testRelationSlicing()
    {
        final CountryShard countryShard = CountryShard.forName("ABC_1-0-0");

        final File boundary = new File(
                "/Users/jamesgage/Desktop/relationSlicingTest/simpleBoundaryFile.txt");
        final CountryBoundaryMap boundaryMap = CountryBoundaryMap.fromPlainText(boundary);

        final HadoopAtlasFileCache lineSlicedAtlasCache = new HadoopAtlasFileCache(
                "/Users/jamesgage/Desktop/relationSlicingTest/atlas/",
                new HashMap<String, String>());

        // final Function<Shard, Optional<Atlas>> atlasFetcher = AtlasGeneratorHelper
        // .atlasFetcher(lineSlicedAtlasCache, boundaryMap, "ABC", countryShard.getShard());

    }
}
