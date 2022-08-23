package org.openstreetmap.atlas.generator.tools.streaming.resource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.StringResource;

/**
 * @author matthieun
 */
public class ResourceFileSystemTest
{
    @Test
    public void listStatusTest()
    {
        ResourceFileSystem.clear();

        final String simpleFilePath = ResourceFileSystem.SCHEME + "://test/path/file1";

        final String longDirectoryPath = ResourceFileSystem.SCHEME
                + "://test/path/with/lots/of/directories/file2";
        final String intermediatePath = ResourceFileSystem.SCHEME + "://test/path/with/lots";
        final String intermediatePathAndChild = ResourceFileSystem.SCHEME
                + "://test/path/with/lots/of";

        final String intermediatePath2 = ResourceFileSystem.SCHEME
                + "://test/path/with/lots/of/directories";

        ResourceFileSystem.addResource(longDirectoryPath, new StringResource("123"));
        ResourceFileSystem.addResource(simpleFilePath, new StringResource("456"));

        final FileSystem fileSystem = new FileSystemCreator().get(simpleFilePath,
                FileSystemCreator.resourceFileSystemScheme());
        try
        {
            List<FileStatus> fileStatuses = Arrays
                    .asList(fileSystem.listStatus(new Path(simpleFilePath)));
            Assert.assertEquals(1, fileStatuses.size());
            Assert.assertEquals(simpleFilePath, fileStatuses.get(0).getPath().toString());
            Assert.assertFalse(fileStatuses.get(0).isDirectory());

            fileStatuses = Arrays.asList(
                    fileSystem.listStatus(new Path(ResourceFileSystem.SCHEME + "://test/path/")));
            Assert.assertEquals(2, fileStatuses.size());

            fileStatuses = Arrays.asList(fileSystem.listStatus(new Path(intermediatePath)));
            Assert.assertEquals(1, fileStatuses.size());
            Assert.assertEquals(intermediatePathAndChild, fileStatuses.get(0).getPath().toString());
            Assert.assertTrue(fileStatuses.get(0).isDirectory());

            fileStatuses = Arrays.asList(fileSystem.listStatus(new Path(intermediatePath2)));
            Assert.assertEquals(1, fileStatuses.size());
            Assert.assertEquals(longDirectoryPath, fileStatuses.get(0).getPath().toString());
            Assert.assertFalse(fileStatuses.get(0).isDirectory());
            Assert.assertEquals(3, fileStatuses.get(0).getLen());

            fileStatuses = Arrays.asList(fileSystem.listStatus(new Path(simpleFilePath)));
            Assert.assertEquals(1, fileStatuses.size());
            Assert.assertEquals(simpleFilePath, fileStatuses.get(0).getPath().toString());
            Assert.assertFalse(fileStatuses.get(0).isDirectory());
            Assert.assertEquals(3, fileStatuses.get(0).getLen());
        }
        catch (final IOException e)
        {
            throw new CoreException("Unexpected exception in listStatusTest", e);
        }
    }

    @Test
    public void testDumpToDisk()
    {
        ResourceFileSystem.addResource(ResourceFileSystem.SCHEME + "://test/123",
                new StringResource("123"));
        ResourceFileSystem.addResource(ResourceFileSystem.SCHEME + "://test/abc/456",
                new StringResource("456"));
        final File tempFolder = File.temporaryFolder();
        try
        {
            tempFolder.mkdirs();
            ResourceFileSystem.dumpToDisk(tempFolder);
            Assert.assertEquals("123", tempFolder.child("test/123").all());
            Assert.assertEquals("456", tempFolder.child("test/abc/456").all());
        }
        finally
        {
            tempFolder.deleteRecursively();
        }
    }
}
