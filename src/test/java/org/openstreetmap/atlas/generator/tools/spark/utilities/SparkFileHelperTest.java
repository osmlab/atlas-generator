package org.openstreetmap.atlas.generator.tools.spark.utilities;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.Resource;

import com.google.common.base.Objects;
import com.google.common.io.Files;

/**
 * Tests for {@link SparkFileHelper}.
 *
 * @author mkalender
 */
public class SparkFileHelperTest
{
    // A helper without any specific configuration
    // Empty configuration targets local filesystem
    private static final SparkFileHelper TEST_HELPER = new SparkFileHelper(Collections.emptyMap());

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCommitDirectory() throws IOException
    {
        // Create temporary files
        // Delete one and rename one to another
        final File tempFolder = this.temporaryFolder.newFolder();
        final File targetFolder = this.temporaryFolder.newFolder();

        targetFolder.delete();

        final File tempFile = File.createTempFile("test", FileSuffix.TEMPORARY.toString(),
                tempFolder);
        final File tempFile2 = File.createTempFile("test-another", FileSuffix.TEMPORARY.toString(),
                tempFolder);

        TEST_HELPER.commit(
                new SparkFilePath(tempFolder.getAbsolutePath(), targetFolder.getAbsolutePath()));

        Assert.assertFalse(tempFile.exists());
        Assert.assertFalse(tempFile2.exists());

        final String[] targetFile = targetFolder
                .list((dir, name) -> name.equals(tempFile.getName()));
        Assert.assertNotNull(targetFile);
        Assert.assertEquals(1, targetFile.length);

        final String[] targetFile2 = targetFolder
                .list((dir, name) -> name.equals(tempFile.getName()));
        Assert.assertNotNull(targetFile2);
        Assert.assertEquals(1, targetFile2.length);
    }

    @Test
    public void testCommitDirectoryByCopy() throws IOException
    {
        // Create temporary files
        // Delete one and copy one to another
        final File tempFolder = this.temporaryFolder.newFolder();
        final File targetFolder = this.temporaryFolder.newFolder();

        targetFolder.delete();

        final File tempFile = File.createTempFile("test", FileSuffix.TEMPORARY.toString(),
                tempFolder);
        final File tempFile2 = File.createTempFile("test-another", FileSuffix.TEMPORARY.toString(),
                tempFolder);

        TEST_HELPER.commitByCopy(
                new SparkFilePath(tempFolder.getAbsolutePath(), targetFolder.getAbsolutePath()));

        Assert.assertTrue(tempFile.exists());
        Assert.assertTrue(tempFile2.exists());

        final String[] targetFile = targetFolder
                .list((dir, name) -> name.equals(tempFile.getName()));
        Assert.assertNotNull(targetFile);
        Assert.assertEquals(1, targetFile.length);

        final String[] targetFile2 = targetFolder
                .list((dir, name) -> name.equals(tempFile.getName()));
        Assert.assertNotNull(targetFile2);
        Assert.assertEquals(1, targetFile2.length);
    }

    @Test
    public void testCommitFile() throws IOException
    {
        // Create temporary files
        // Delete one and rename one to another
        final File tempFile = File.createTempFile("test", FileSuffix.TEMPORARY.toString());
        tempFile.deleteOnExit();
        Assert.assertTrue(tempFile.exists());

        final File tempFile2 = File.createTempFile("test-another", FileSuffix.TEMPORARY.toString());
        tempFile2.delete();
        Assert.assertFalse(tempFile2.exists());

        // Rename test to test-another
        TEST_HELPER
                .commit(new SparkFilePath(tempFile.getAbsolutePath(), tempFile2.getAbsolutePath()));
        Assert.assertFalse(tempFile.exists());
        Assert.assertTrue(tempFile2.exists());
    }

    @Test
    public void testCommitFileByFile() throws IOException
    {
        // Create temporary files
        // Delete one and copy one to another
        final File tempFile = File.createTempFile("test", FileSuffix.TEMPORARY.toString());
        tempFile.deleteOnExit();
        Assert.assertTrue(tempFile.exists());

        final File tempFile2 = File.createTempFile("test-another", FileSuffix.TEMPORARY.toString());
        tempFile2.delete();
        Assert.assertFalse(tempFile2.exists());

        // Rename test to test-another
        TEST_HELPER.commitByCopy(
                new SparkFilePath(tempFile.getAbsolutePath(), tempFile2.getAbsolutePath()));
        Assert.assertTrue(tempFile.exists());
        Assert.assertTrue(tempFile2.exists());
    }

    @Test(expected = CoreException.class)
    public void testDeleteNonExistingDirectory()
    {
        // Generate a temp path
        final File tempFolder = Files.createTempDir();
        final String path = tempFolder.getAbsolutePath();
        tempFolder.delete();

        // Try to delete directory
        TEST_HELPER.deleteDirectory(path);
    }

    @Test
    public void testIsDirectory() throws IOException
    {
        // Test a file
        final File tempFile = File.createTempFile("test", FileSuffix.TEMPORARY.toString());
        tempFile.deleteOnExit();
        Assert.assertFalse(TEST_HELPER.isDirectory(tempFile.getAbsolutePath()));

        // Test a folder
        final File tempFolder = Files.createTempDir();
        tempFolder.deleteOnExit();
        Assert.assertTrue(TEST_HELPER.isDirectory(tempFolder.getAbsolutePath()));
    }

    @Test
    public void testList() throws IOException
    {
        // Start with an empty folder
        final File tempFolder = Files.createTempDir();
        tempFolder.deleteOnExit();
        Assert.assertTrue(TEST_HELPER.list(tempFolder.getAbsolutePath()).isEmpty());

        // Add a file and then delete
        final File tempFile1 = File.createTempFile("test1", FileSuffix.TEMPORARY.toString(),
                tempFolder);
        Assert.assertEquals(1, TEST_HELPER.list(tempFolder.getAbsolutePath()).size());
        tempFile1.delete();
        Assert.assertTrue(TEST_HELPER.list(tempFolder.getAbsolutePath()).isEmpty());

        // Generate random number of files
        final int randomFileCount = new Random().nextInt(50) + 1;
        final File[] randomFiles = new File[randomFileCount];
        for (int index = 0; index < randomFileCount; index++)
        {
            randomFiles[index] = File.createTempFile("test" + index,
                    FileSuffix.TEMPORARY.toString(), tempFolder);
            Assert.assertEquals(index + 1, TEST_HELPER.list(tempFolder.getAbsolutePath()).size());
        }

        Assert.assertEquals(randomFileCount, TEST_HELPER.list(tempFolder.getAbsolutePath()).size());

        // Go over and make sure files are there
        final List<Resource> files = TEST_HELPER.list(tempFolder.getAbsolutePath());
        for (final File randomFile : randomFiles)
        {
            Assert.assertTrue(
                    files.stream().anyMatch(resource -> Objects.equal(randomFile.getPath(),
                            SparkFileHelper.combine(tempFolder.getPath(), resource.getName()))));
        }
    }

    @Test
    public void testMkdirAndDeleteDirectory()
    {
        // Generate a temp path
        final File tempFolder = Files.createTempDir();
        final String path = tempFolder.getAbsolutePath();
        tempFolder.delete();

        // Make sure folder doesn't exist yet
        Assert.assertFalse(TEST_HELPER.isDirectory(path));
        Assert.assertFalse(new File(path).exists());

        // Create directory
        TEST_HELPER.mkdir(path);
        Assert.assertTrue(TEST_HELPER.isDirectory(path));
        Assert.assertTrue(new File(path).exists());

        // Delete directory
        TEST_HELPER.deleteDirectory(path);
        Assert.assertFalse(TEST_HELPER.isDirectory(path));
        Assert.assertFalse(new File(path).exists());
    }

    @Test
    public void testPathOperations()
    {
        final String dir1 = "dir1";
        final String dir2 = "dir2";
        final String combined1 = SparkFileHelper.combine(dir1, dir2);
        Assert.assertEquals("dir1/dir2", combined1);
        Assert.assertEquals("dir1", SparkFileHelper.parentPath(combined1));

        final String combined2 = SparkFileHelper.combine(dir1, dir2, dir2, dir1);
        Assert.assertEquals("dir1/dir2/dir2/dir1", combined2);
        Assert.assertEquals("dir1/dir2/dir2", SparkFileHelper.parentPath(combined2));

        final String dir3 = "dir3/";
        final String dir4 = "/dir4";
        final String combined3 = SparkFileHelper.combine(dir3, dir4);
        Assert.assertEquals("dir3/dir4", combined3);
        Assert.assertEquals("dir3", SparkFileHelper.parentPath(combined3));

        final String dir5 = "dir5////";
        final String dir6 = "dir6";
        final String combined4 = SparkFileHelper.combine(dir5, dir6, dir5, dir6);
        Assert.assertEquals("dir5/dir6/dir5/dir6", combined4);

        final String combined5 = SparkFileHelper.combine(dir5, dir6, dir5);
        Assert.assertEquals("dir5/dir6/dir5", combined5);

        final String dir7 = "dir7////";
        final String dir8 = "//////dir8";
        final String combined6 = SparkFileHelper.combine(dir7, dir8);
        Assert.assertEquals("dir7/dir8", combined6);

        final String combined7 = SparkFileHelper.combine(dir1, dir2, dir3, dir4, dir5, dir6, dir7,
                dir8, dir7, dir6, dir5, dir4, dir3, dir2, dir1);
        Assert.assertEquals(
                "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir7/dir6/dir5/dir4/dir3/dir2/dir1",
                combined7);

        final String prefix = "hdfs://somePath/";
        final String suffix = "/anotherComponent//";
        Assert.assertEquals("hdfs://somePath/anotherComponent",
                SparkFileHelper.combine(prefix, suffix));
    }

    @Test
    public void testRename() throws IOException
    {
        // Create temporary files
        // Delete one and rename one to another
        final File tempFile = File.createTempFile("test", FileSuffix.TEMPORARY.toString());
        tempFile.deleteOnExit();
        Assert.assertTrue(tempFile.exists());

        final File tempFile2 = File.createTempFile("test-another", FileSuffix.TEMPORARY.toString());
        tempFile2.delete();
        Assert.assertFalse(tempFile2.exists());

        // Rename test to test-another
        TEST_HELPER.rename(tempFile.getAbsolutePath(), tempFile2.getAbsolutePath());
        Assert.assertFalse(tempFile.exists());
        Assert.assertTrue(tempFile2.exists());
    }

    @Test
    public void testWrite() throws IOException
    {
        final String testBytesFileName = "write_test_bytes";
        final String testStringFileName = "write_test_string";
        final byte[] testBytes = { 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, };
        final String testString = "OsmRocks!";
        final File testFolder = this.temporaryFolder.newFolder();
        TEST_HELPER.write(testFolder.getAbsolutePath(), testBytesFileName, testBytes);

        final String testByteResource = SparkFileHelper.combine(testFolder.getAbsolutePath(),
                testBytesFileName);
        Assert.assertArrayEquals("SparkFileHelper write with byteArray failed", testBytes,
                FileSystemHelper.resource(testByteResource).readBytesAndClose());

        TEST_HELPER.write(testFolder.getAbsolutePath(), testStringFileName, testString);
        final String testStringResource = SparkFileHelper.combine(testFolder.getAbsolutePath(),
                testStringFileName);
        Assert.assertEquals("SparkFileHelper write with string failed", testString,
                FileSystemHelper.resource(testStringResource).all());
    }
}
