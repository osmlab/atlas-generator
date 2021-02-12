package org.openstreetmap.atlas.mutator.command;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.FileSystem;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.collections.Maps;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

/**
 * @author lcram
 */
public class ConfigurationInfoCommandTest
{
    @Test
    public void testConfigLoadFailures()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystem1(filesystem);
            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            ConfigurationInfoCommand command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent));
            command.setNewErrStream(new PrintStream(errContent));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-json={}", "--countries=AIA", "--verbose", "print");
            Assert.assertEquals("config-info: loading config resources...\n"
                    + "config-info: error: supplied configuration was empty, please check logs for details\n",
                    errContent.toString());

            final ByteArrayOutputStream outContent2 = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent2 = new ByteArrayOutputStream();
            command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent2));
            command.setNewErrStream(new PrintStream(errContent2));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-resource=/Users/foo/bar.json", "--countries=AIA",
                    "--verbose", "print");
            Assert.assertEquals("config-info: loading config resources...\n"
                    + "config-info: error: failed to fetch Configuration object, please check logs for details\n",
                    errContent2.toString());

            final ByteArrayOutputStream outContent3 = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent3 = new ByteArrayOutputStream();
            command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent3));
            command.setNewErrStream(new PrintStream(errContent3));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--countries=AIA", "--verbose", "print");
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: error: no configurations were supplied\n",
                    errContent3.toString());

            final ByteArrayOutputStream outContent4 = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent4 = new ByteArrayOutputStream();
            command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent4));
            command.setNewErrStream(new PrintStream(errContent4));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-json={\"foo\":\"bar\"}", "--countries=AIA", "--verbose",
                    "print");
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: constructing configuration...\n"
                            + "config-info: error: the AtlasMutatorConfiguration was empty\n",
                    errContent4.toString());

            final ByteArrayOutputStream outContent5 = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent5 = new ByteArrayOutputStream();
            command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent5));
            command.setNewErrStream(new PrintStream(errContent5));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-json={\"foo\":\"bar\"}", "--countries=AIA", "--verbose",
                    "details");
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: constructing configuration...\n"
                            + "config-info: error: the AtlasMutatorConfiguration was empty\n",
                    errContent5.toString());

            final ByteArrayOutputStream outContent6 = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent6 = new ByteArrayOutputStream();
            command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent6));
            command.setNewErrStream(new PrintStream(errContent6));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-json={\"foo\":\"bar\"}", "--countries=AIA", "--verbose",
                    "interactive");
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: starting interactive mode...\n"
                            + "config-info: constructing configuration...\n"
                            + "config-info: error: the AtlasMutatorConfiguration was empty\n",
                    errContent6.toString());
        }
        catch (final IOException exception)
        {
            throw new CoreException("FileSystem operation failed", exception);
        }
    }

    @Test
    public void testDetailsMode()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystem1(filesystem);
            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            ConfigurationInfoCommand command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent));
            command.setNewErrStream(new PrintStream(errContent));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-resource=/Users/foo/config.json", "--countries=AIA",
                    "--verbose", "details");

            Assert.assertEquals(new File("/Users/foo/expectedDetails.txt", filesystem).all() + "\n",
                    outContent.toString());
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: constructing configuration...\n",
                    errContent.toString());

            final ByteArrayOutputStream outContent2 = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent2 = new ByteArrayOutputStream();
            command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent2));
            command.setNewErrStream(new PrintStream(errContent2));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-resource=/Users/foo/config.json", "--countries=AIA",
                    "--verbose", "details", "AIA");

            Assert.assertEquals(
                    new File("/Users/foo/expectedDetails2.txt", filesystem).all() + "\n",
                    outContent2.toString());
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: constructing configuration...\n",
                    errContent2.toString());
        }
        catch (final IOException exception)
        {
            throw new CoreException("FileSystem operation failed", exception);
        }
    }

    @Test
    public void testInteractiveMode()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystem1(filesystem);
            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            final InputStream inStream = new StringResource("AIA\n").read();
            final ConfigurationInfoCommand command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent));
            command.setNewErrStream(new PrintStream(errContent));
            command.setNewInStream(inStream);
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-resource=/Users/foo/config.json", "--countries=AIA",
                    "--verbose", "--disable-jline", "interactive");

            Assert.assertEquals(new File("/Users/foo/expectedInteractive.txt", filesystem).all(),
                    outContent.toString());
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: starting interactive mode...\n"
                            + "config-info: constructing configuration...\n",
                    errContent.toString());
        }
        catch (final IOException exception)
        {
            throw new CoreException("FileSystem operation failed", exception);
        }
    }

    @Test
    public void testPrintMode()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystem1(filesystem);
            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            ConfigurationInfoCommand command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent));
            command.setNewErrStream(new PrintStream(errContent));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-resource=/Users/foo/config.json", "--countries=AIA",
                    "--verbose", "print");

            Assert.assertEquals(new File("/Users/foo/expectedPrint.txt", filesystem).all() + "\n\n",
                    outContent.toString());
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: constructing configuration...\n",
                    errContent.toString());

            final ByteArrayOutputStream outContent2 = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent2 = new ByteArrayOutputStream();
            command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent2));
            command.setNewErrStream(new PrintStream(errContent2));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-resource=/Users/foo/config.json", "--countries=AIA",
                    "--verbose", "print", "AIA");

            Assert.assertEquals(new File("/Users/foo/expectedPrint.txt", filesystem).all() + "\n\n",
                    outContent2.toString());
            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: constructing configuration...\n",
                    errContent2.toString());

            final ByteArrayOutputStream outContent3 = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent3 = new ByteArrayOutputStream();
            command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent3));
            command.setNewErrStream(new PrintStream(errContent3));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            command.runSubcommand("--config-resource=/Users/foo/config.json", "--countries=AIA",
                    "--verbose", "print", "FOOBAR");

            Assert.assertEquals(
                    "config-info: loading config resources...\n"
                            + "config-info: constructing configuration...\n"
                            + "config-info: warn: FOOBAR was not found in the configuration\n",
                    errContent3.toString());
        }
        catch (final IOException exception)
        {
            throw new CoreException("FileSystem operation failed", exception);
        }
    }

    @Test
    public void testUnknownMode()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystem1(filesystem);
            final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            final ConfigurationInfoCommand command = new ConfigurationInfoCommand();
            command.setNewFileSystem(filesystem);
            command.setNewOutStream(new PrintStream(outContent));
            command.setNewErrStream(new PrintStream(errContent));
            command.setNewEnvironment(Maps.hashMap("HOME", "/Users/foo"));
            final int returnCode = command.runSubcommand("--config-resource=/Users/foo/config.json",
                    "--countries=AIA", "--verbose", "foo");

            Assert.assertEquals("config-info: loading config resources...\n"
                    + "config-info: error: unknown mode 'foo': try 'print', 'details', or 'interactive'\n",
                    errContent.toString());
            Assert.assertEquals(1, returnCode);
        }
        catch (final IOException exception)
        {
            throw new CoreException("FileSystem operation failed", exception);
        }
    }

    private void setupFilesystem1(final FileSystem filesystem) throws IOException
    {
        final File boundaryFile = new File(filesystem.getPath("/Users/foo", "config.json"));
        boundaryFile.writeAndClose(ConfigurationInfoCommandTest.class
                .getResourceAsStream("ConfigurationInfoCommandTest.json").readAllBytes());

        final File expectedDetailsFile = new File(
                filesystem.getPath("/Users/foo", "expectedDetails.txt"));
        expectedDetailsFile.writeAndClose(ConfigurationInfoCommandTest.class
                .getResourceAsStream("ConfigurationInfoCommandTest_ExpectedDetails.json")
                .readAllBytes());
        final File expectedDetails2File = new File(
                filesystem.getPath("/Users/foo", "expectedDetails2.txt"));
        expectedDetails2File.writeAndClose(ConfigurationInfoCommandTest.class
                .getResourceAsStream("ConfigurationInfoCommandTest_ExpectedDetails2.txt")
                .readAllBytes());

        final File expectedPrintFile = new File(
                filesystem.getPath("/Users/foo", "expectedPrint.txt"));
        expectedPrintFile.writeAndClose(ConfigurationInfoCommandTest.class
                .getResourceAsStream("ConfigurationInfoCommandTest_ExpectedPrint.txt")
                .readAllBytes());

        final File expectedInteractiveFile = new File(
                filesystem.getPath("/Users/foo", "expectedInteractive.txt"));
        expectedInteractiveFile.writeAndClose(ConfigurationInfoCommandTest.class
                .getResourceAsStream("ConfigurationInfoCommandTest_ExpectedInteractive.txt")
                .readAllBytes());
    }
}
