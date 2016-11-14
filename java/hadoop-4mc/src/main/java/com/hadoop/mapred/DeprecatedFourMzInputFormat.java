package com.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.hadoop.compression.fourmc.FourMzBlockIndex;
import com.hadoop.compression.fourmc.FourMcInputFormatUtil;
import com.hadoop.compression.fourmc.util.HadoopUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for 4mc compressed files.
 * This is the base class, mainly managing input splits, leveraging 4mc block index.
 *
 * Subclasses must only make sure to provide an implementation of createRecordReader.
 * See {@link com.hadoop.mapreduce.FourMcTextInputFormat} as example reading text files.
 * 
 * <b>Note:</b> unlikely default hadoop, but exactly like the EB version
 * this recursively examines directories for matching files.
 */
public class DeprecatedFourMzInputFormat extends TextInputFormat {
    private static final Log LOG = LogFactory.getLog(DeprecatedFourMzInputFormat.class.getName());

    private final PathFilter hiddenPathFilter = new PathFilter() {
        // avoid hidden files and directories.

        public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith(".") &&
                    !name.startsWith("_");
        }
    };

    private final PathFilter visible4mcFilter = new PathFilter() {
        public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith(".") &&
                    !name.startsWith("_") &&
                    FourMcInputFormatUtil.is4mzFile(name);
        }
    };

    @Override
    protected FileStatus[] listStatus(JobConf conf) throws IOException {
        List<FileStatus> files = new ArrayList<FileStatus>(Arrays.asList(super.listStatus(conf)));
        
        List<FileStatus> results = new ArrayList<FileStatus>();
        boolean recursive = conf.getBoolean("mapred.input.dir.recursive", false);

        Iterator<FileStatus> it = files.iterator();
        while (it.hasNext()) {
            FileStatus fileStatus = it.next();
            FileSystem fs = fileStatus.getPath().getFileSystem(conf);
            addInputPath(results, fs, fileStatus, recursive);
        }

        LOG.debug("Total 4mc input paths to process: " + results.size());
        return results.toArray(new FileStatus[results.size()]);
    }

    protected void addInputPath(List<FileStatus> results, FileSystem fs,
                                FileStatus pathStat, boolean recursive) throws IOException {
        Path path = pathStat.getPath();
        if (pathStat.isDir()) {
            if (recursive) {
                for (FileStatus stat : fs.listStatus(path, hiddenPathFilter)) {
                    addInputPath(results, fs, stat, recursive);
                }
            }
        } else if (visible4mcFilter.accept(path)) {
            results.add(pathStat);
        }
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return true;
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
        
        FileSplit[] splits = (FileSplit[])super.getSplits(conf, numSplits);

        List<FileSplit> result = new ArrayList<FileSplit>();

        Path prevFile = null;
        FourMzBlockIndex prevIndex = null;

        for (FileSplit fileSplit: splits) {
            // Load the index.
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);

            FourMzBlockIndex index;
            if (file.equals(prevFile)) {
                index = prevIndex;
            } else {
                index = FourMzBlockIndex.readIndex(fs, file);
                prevFile = file;
                prevIndex = index;
            }

            if (index == null) {
                throw new IOException("BlockIndex unreadable for " + file);
            }

            if (index.isEmpty()) { // leave the default split for empty block index
                result.add(fileSplit);
                continue;
            }

            long start = fileSplit.getStart();
            long end = start + fileSplit.getLength();

            long fourMcStart = index.alignSliceStartToIndex(start, end);
            long fourMcEnd = index.alignSliceEndToIndex(end, fs.getFileStatus(file).getLen());

            if (fourMcStart != FourMzBlockIndex.NOT_FOUND && fourMcEnd != FourMzBlockIndex.NOT_FOUND) {
                result.add(new FileSplit(file, fourMcStart, fourMcEnd - fourMcStart, fileSplit.getLocations()));
                LOG.debug("Added 4mc split for " + file + "[start=" + fourMcStart + ", length=" + (fourMcEnd - fourMcStart) + "]");
            }

        }

        return result.toArray(new FileSplit[result.size()]);
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException  {
        return new DeprecatedFourMzLineRecordReader(split, conf);
    }
}