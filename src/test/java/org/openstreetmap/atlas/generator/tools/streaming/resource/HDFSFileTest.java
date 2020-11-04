package org.openstreetmap.atlas.generator.tools.streaming.resource;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openstreetmap.atlas.exception.CoreException;

/**
 * Test class for {@link HDFSFile}
 *
 * @author Taylor Smock
 */
public class HDFSFileTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testDirectory() throws IOException
    {
        final File tempFile = this.temporaryFolder.newFile();
        final HDFSFile file = new HDFSFile(tempFile.getPath());
        assertFalse(file.isDirectory());
        assertTrue(tempFile.delete());
        assertTrue(tempFile.mkdir());
        assertTrue(file.isDirectory());
    }

    @Test
    public void testFileExists() throws IOException
    {
        final File temp = this.temporaryFolder.newFile();
        final HDFSFile file = new HDFSFile(temp.getPath());
        assertTrue(file.exists());
        assertTrue(temp.delete());
        assertFalse(file.exists());
    }

    @Test
    public void testLength() throws IOException
    {
        final File temp = this.temporaryFolder.newFile();
        final HDFSFile file = new HDFSFile(temp.getPath());
        file.setRetries(0);
        try (OutputStream outputStream = file.onWrite())
        {
            outputStream.write(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 });
        }
        assertEquals(9, file.length());
        file.remove(true);
        try
        {
            file.length();
        }
        catch (final CoreException e)
        {
            assertTrue(e.getMessage().contains("Could not check length of path"));
        }
    }

    @Test
    public void testReadWrite() throws IOException
    {
        final File temp = this.temporaryFolder.newFile();
        final HDFSFile file = new HDFSFile(temp.getPath());
        final byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 };
        try (OutputStream outputStream = file.onWrite())
        {
            outputStream.write(bytes);
        }
        try (InputStream inputStream = file.onRead())
        {
            final byte[] readBytes = inputStream.readAllBytes();
            assertArrayEquals(bytes, readBytes);
        }
    }

    @Test
    public void testReadWriteConcat() throws IOException
    {
        final File temp = this.temporaryFolder.newFile();
        final HDFSFile file = new HDFSFile(temp.getPath());
        file.setRetries(0);
        final byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 };
        try (OutputStream outputStream = file.onWrite())
        {
            outputStream.write(bytes);
        }
        final byte[] bytes2 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 10 };
        assertFalse(file.isAppendToFile());
        try (OutputStream outputStream = file.onWrite())
        {
            outputStream.write(bytes2);
        }
        try (InputStream inputStream = file.onRead())
        {
            final byte[] readBytes = inputStream.readAllBytes();
            assertArrayEquals(bytes2, readBytes);
        }

        file.setAppendToFile(true);
        assertTrue(file.isAppendToFile());
        try (OutputStream outputStream = file.onWrite())
        {
            outputStream.write(bytes);
        }
        catch (final CoreException e)
        {
            Throwable throwable = e.getCause();
            while (throwable instanceof CoreException)
            {
                throwable = throwable.getCause();
            }
            assertSame(IOException.class, throwable.getClass());
            assertEquals("Not supported", throwable.getMessage());
            // Appending is not currently supported, but when it is, the code outside this catch
            // block will run
            return;
        }
        try (InputStream inputStream = file.onRead())
        {
            final Collection<Byte> expected = new ArrayList<>();
            for (final byte byt : bytes2)
            {
                expected.add(byt);
            }
            for (final byte byt : bytes)
            {
                expected.add(byt);
            }
            final Collection<Byte> readBytes = new ArrayList<>();
            for (final byte byt : inputStream.readAllBytes())
            {
                readBytes.add(byt);
            }
            assertArrayEquals(expected.toArray(Byte[]::new), readBytes.toArray(Byte[]::new));
        }
        throw new AssertionError("Appending may be supported. Please modify this test.");
    }

    @Test
    public void testRemove() throws IOException
    {
        final File temp = this.temporaryFolder.newFile("gzipped.gz");
        final HDFSFile file = new HDFSFile(temp.getPath());
        file.setRetries(0);
        assertTrue(file.exists());
        file.remove(false);
        assertFalse(file.exists());
        file.mkdirs(false);
        final File temp2 = new File(temp, "random_file");
        assertTrue(temp2.mkdir());
        assertTrue(file.exists());
        assertTrue(temp2.exists());
        // This would be better with JUnit 5 assertThrows
        try
        {
            file.remove(false);
        }
        catch (final CoreException e)
        {
            // Core exceptions should have an inner exception
            assertTrue(
                    e.getCause().getMessage().contains("Could not delete path in HDFS location"));
            assertTrue(file.exists());
            assertTrue(temp2.exists());
        }
        file.remove(true);
        assertFalse(file.exists());
        assertFalse(temp2.exists());
    }
}
