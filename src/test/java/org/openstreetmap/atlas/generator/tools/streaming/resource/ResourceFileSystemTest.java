package org.openstreetmap.atlas.generator.tools.streaming.resource;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.StringResource;

/**
 * @author matthieun
 */
public class ResourceFileSystemTest
{
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
