package org.openstreetmap.atlas.generator.tools.streaming.resource;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.utilities.tuples.Tuple;

/**
 * Tests for {@link HDFSWalker}.
 *
 * @author mkalender
 */
public class HDFSWalkerTest
{
    private static Tuple<List<FileStatus>, List<String>> test(final File directory)
    {
        final List<FileStatus> fileStatusList = new ArrayList<>();
        final List<String> debugStrings = new ArrayList<>();
        HDFSWalker.walk(new Path(directory.getPath())).map(HDFSWalker.debug(debugStrings::add))
                .forEach(status ->
                {
                    fileStatusList.add(status);
                });
        return Tuple.createTuple(fileStatusList, debugStrings);
    }

    @Test
    public void testDirectoryWithAFile()
    {
        final File directory = File.temporaryFolder();
        directory.child("test.tmp").writeAndClose("test file");
        final Tuple<List<FileStatus>, List<String>> results = test(directory);

        // Verify file statuses
        Assert.assertFalse(results.getFirst().isEmpty());
        Assert.assertEquals(1, results.getFirst().size());

        // Verify debug strings
        Assert.assertFalse(results.getSecond().isEmpty());
        Assert.assertEquals(1, results.getSecond().size());
        Assert.assertEquals(
                String.format("[F] file:%s/%s", directory.getAbsolutePath(), "test.tmp"),
                results.getSecond().get(0));

        // Clean up
        directory.deleteRecursively();
    }

    @Test
    public void testDirectoryWithAFileInsideAChildDirectory()
    {
        final File directory = File.temporaryFolder();
        directory.child("test.tmp").writeAndClose("test file");
        final File childDirectory = directory.child("child-directory");
        Assert.assertTrue(childDirectory.mkdirs());
        childDirectory.child("file-in-child-directory.tmp").writeAndClose("test child file");

        // Verify file statuses
        final Tuple<List<FileStatus>, List<String>> results = test(directory);
        Assert.assertFalse(results.getFirst().isEmpty());
        Assert.assertEquals(3, results.getFirst().size());

        // Verify debug strings
        Assert.assertFalse(results.getSecond().isEmpty());
        Assert.assertEquals(3, results.getSecond().size());
        Assert.assertTrue(results.getSecond().contains(
                String.format("[D] file:%s/%s", directory.getAbsolutePath(), "child-directory")));
        Assert.assertTrue(results.getSecond().contains(String.format("[F] file:%s/%s",
                childDirectory.getAbsolutePath(), "file-in-child-directory.tmp")));
        Assert.assertTrue(results.getSecond().contains(
                String.format("[F] file:%s/%s", directory.getAbsolutePath(), "test.tmp")));

        // Clean up
        directory.deleteRecursively();
    }

    @Test
    public void testEmptyDirectory()
    {
        final File emptyDirectory = File.temporaryFolder();
        final Tuple<List<FileStatus>, List<String>> results = test(emptyDirectory);

        // Verify status and debug strings are empty
        Assert.assertTrue(results.getFirst().isEmpty());
        Assert.assertTrue(results.getSecond().isEmpty());
        emptyDirectory.deleteRecursively();
    }
}
