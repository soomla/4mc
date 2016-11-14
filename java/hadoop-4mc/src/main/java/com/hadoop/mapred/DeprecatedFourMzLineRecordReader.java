package com.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapred.InputSplit;


/**
 * Reads line from a 4mz compressed text file.
 * Treats keys as offset in file and value as line.
 */
public class DeprecatedFourMzLineRecordReader implements RecordReader<LongWritable, Text> {

    public static final String MAX_LINE_LEN_CONF = "com.hadoop.mapreduce.fourmc.line.recordreader.max.line.length";

    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private FSDataInputStream fileIn;

    private int maxLineLen = Integer.MAX_VALUE;

    public String getName() {
        return "fourmz";
    }

    public String getFilePattern() {
        return "\\.4mz&";
    }

    public DeprecatedFourMzLineRecordReader(InputSplit genericSplit, Configuration job) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        maxLineLen = job.getInt(MAX_LINE_LEN_CONF, Integer.MAX_VALUE);

        FileSystem fs = file.getFileSystem(job);
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (codec == null) {
            throw new IOException("Codec for file " + file + " not found, cannot run");
        }

        // open the file and seek to the start of the split
        fileIn = fs.open(file);

        // creates input stream and also reads the file header
        in = new LineReader(codec.createInputStream(fileIn), job);

        if (start != 0) {
            fileIn.seek(start);

            // read and ignore the first line
            in.readLine(new Text());
            start = fileIn.getPos();
        }

        this.pos = start;
    }

    /**
     * Get the progress within the split.
     */
    @Override
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
        // exactly same as EB one
        if (pos <= end) {
            key.set(pos);

            int newSize = in.readLine(value, maxLineLen);
            if (newSize == 0) {
                return false;
            }
            pos = fileIn.getPos();

            return true;
        }
        return false;
    }
}