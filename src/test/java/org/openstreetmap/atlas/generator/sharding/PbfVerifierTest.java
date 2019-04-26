package org.openstreetmap.atlas.generator.sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;

/**
 * Tests functionality of the PbfVerifier.
 *
 * @author jamesgage
 */
public class PbfVerifierTest
{
    private final Resource slippyTileFile = new InputStreamResource(
            () -> PbfVerifierTest.class.getResourceAsStream("testSlippyTile"));
    private final Resource pbf1 = new InputStreamResource(
            () -> PbfVerifierTest.class.getResourceAsStream("10-313-380.pbf"));
    private final Resource pbf2 = new InputStreamResource(
            () -> PbfVerifierTest.class.getResourceAsStream("10-313-381.pbf"));

    @Test
    public void testBuildAllPbfs()
    {
        // create a list of pbf resources
        final List<Resource> pbfFiles = new ArrayList<>();
        pbfFiles.add(this.pbf1);
        pbfFiles.add(this.pbf2);

        final PbfVerifier pbfVerifier = new PbfVerifier();
        // attempt to build them all
        final int returnCode = pbfVerifier.buildAllPbfs(pbfFiles);
        Assert.assertEquals(0, returnCode);
    }

    @Test
    public void testCheckForMissingPbfs()
    {
        // create a list of pbf resources
        final List<Resource> pbfFiles = new ArrayList<>();
        pbfFiles.add(this.pbf1);
        pbfFiles.add(this.pbf2);

        final HashMap<String, Rectangle> shardToBounds = PbfVerifier
                .parseSlippyTileFile(this.slippyTileFile);

        final PbfVerifier pbfVerifier = new PbfVerifier();

        final List<String> pbfFileNames = new ArrayList<>();
        pbfFileNames.add("10-313-380.pbf");
        pbfFileNames.add("10-313-381.pbf");

        final Integer expectedPbfCount = Integer.parseInt(this.slippyTileFile.firstLine());

        final int returnCode = pbfVerifier.checkForMissingPbfs(shardToBounds, pbfFileNames,
                expectedPbfCount);
        Assert.assertEquals(0, returnCode);
    }

    @Test
    public void testParseSlippyTileFile()
    {
        final HashMap<String, Rectangle> shardToBounds = PbfVerifier
                .parseSlippyTileFile(this.slippyTileFile);
        Assert.assertEquals(2, shardToBounds.keySet().size());
    }

}
