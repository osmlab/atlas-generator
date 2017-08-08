package org.openstreetmap.atlas.generator.tools.spark.input;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.openstreetmap.atlas.streaming.Streams;

/**
 * From <a href=
 * "http://stackoverflow.com/questions/5377118/how-to-convert-txt-file-to-hadoops-sequence-file-format">
 * </a> <a href=
 * "http://stackoverflow.com/questions/16070587/reading-and-writing-sequencefile-using-hadoop-2-0-apis">
 * </a>
 *
 * @author matthieun
 */
public final class SequenceFileCreator
{
    public static void main(final String[] args) throws IOException
    {
        final String docDirectoryPath = args[0];
        final String sequenceFilePath = args[1];
        final File docDirectory = new File(docDirectoryPath);

        if (!docDirectory.isDirectory())
        {
            System.out.println(
                    "Please provide an absolute path of a directory that contains the documents to be added to the sequence file");
            return;
        }
        final Configuration conf = new Configuration();

        /*
         * SequenceFile.Writer sequenceFileWriter = SequenceFile.createWriter(fs, conf, new
         * Path(sequenceFilePath), Text.class, BytesWritable.class);
         */
        final org.apache.hadoop.io.SequenceFile.Writer.Option filePath = SequenceFile.Writer
                .file(new Path(sequenceFilePath));
        final org.apache.hadoop.io.SequenceFile.Writer.Option keyClass = SequenceFile.Writer
                .keyClass(Text.class);
        final org.apache.hadoop.io.SequenceFile.Writer.Option valueClass = SequenceFile.Writer
                .valueClass(BytesWritable.class);

        final SequenceFile.Writer sequenceFileWriter = SequenceFile.createWriter(conf, filePath,
                keyClass, valueClass);

        final File[] documents = docDirectory.listFiles();

        for (final File document : documents)
        {

            final RandomAccessFile raf = new RandomAccessFile(document, "r");
            final byte[] content = new byte[(int) raf.length()];

            raf.readFully(content);

            sequenceFileWriter.append(new Text(document.getName()), new BytesWritable(content));

            raf.close();
            Streams.close(sequenceFileWriter);
        }

        // Read the sequence file to make sure
        final org.apache.hadoop.io.SequenceFile.Reader.Option readFilePath = SequenceFile.Reader
                .file(new Path(sequenceFilePath));
        final SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, readFilePath);

        final Writable key = (Writable) ReflectionUtils
                .newInstance(sequenceFileReader.getKeyClass(), conf);
        final Writable value = (Writable) ReflectionUtils
                .newInstance(sequenceFileReader.getValueClass(), conf);

        while (sequenceFileReader.next(key, value))
        {
            System.out.printf("[%s] %s %s \n", sequenceFileReader.getPosition(), key, value);
        }
        Streams.close(sequenceFileReader);
    }

    private SequenceFileCreator()
    {
    }
}
