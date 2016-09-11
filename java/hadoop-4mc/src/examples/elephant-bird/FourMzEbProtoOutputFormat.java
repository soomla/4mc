import com.google.protobuf.Message;
import com.hadoop.compression.fourmc.ZstdCodec;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoBinaryBlockRecordWriter;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Adapter between Elephant-Bird framework and 4mc: this is able to make EB framework handle output binary protobuf
 * to 4mz files: files are seamlessly written by EB in 4mz format, with related internal 4mz block index.
 * Usage:
 * where you usually set EB output as:
 * LzoProtobufBlockOutputFormat.setClassConf(MyProtoMessage.class, HadoopCompat.getConfiguration(job));
 * job.setOutputFormatClass(LzoProtobufBlockOutputFormat.class);
 * replace with:
 * FourMzEbProtoOutputFormat.setOutputFormatClass(MyProtoMessage.class, ZstdCodec.class, job); // or higher codecs
 * That's it.
 *
 * @param <M> protobuf message to be used/written.
 */
public class FourMzEbProtoOutputFormat<M extends Message> extends FileOutputFormat<M, ProtobufWritable<M>> {

    protected TypeRef<M> typeRef;
    private static String CLASS_CONF_KEY = "elephantbird.class.for.FourMzEbProtoOutputFormat";

    protected void setTypeRef(TypeRef<M> typeRef) {
        this.typeRef = typeRef;
    }

    public FourMzEbProtoOutputFormat() {
    }

    public FourMzEbProtoOutputFormat(TypeRef<M> typeRef) {
        this.typeRef = typeRef;
    }

    public static void setOutputFormatClass(Class<?> protoClazz, Class<? extends CompressionCodec> codecClass, Job job) {
        job.setOutputFormatClass(FourMzEbProtoOutputFormat.class);
        FourMzEbProtoOutputFormat.setCompressOutput(job, true);
        FourMzEbProtoOutputFormat.setOutputCompressorClass(job, codecClass);
        setClassConf(protoClazz, job.getConfiguration());
    }


    public static <M extends Message> FourMzEbProtoOutputFormat<M> newInstance(TypeRef<M> typeRef) {
        return new FourMzEbProtoOutputFormat<M>(typeRef);
    }

    @Override
    public RecordWriter<M, ProtobufWritable<M>> getRecordWriter(TaskAttemptContext taskAttempt)
            throws IOException, InterruptedException {

        Configuration conf = taskAttempt.getConfiguration();
        if (typeRef == null) {
            setTypeRef(conf);
        }

        return new LzoBinaryBlockRecordWriter<M, ProtobufWritable<M>>(
                new ProtobufBlockWriter<M>(getOutputStream(taskAttempt), typeRef.getRawClass()));
    }

    /**
     * Helper method to create 4mz wrapped output stream, with requested compression codec
     */
    protected DataOutputStream getOutputStream(TaskAttemptContext job)
            throws IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        CompressionCodec codec = null;
        String extension = "";

        Class<? extends CompressionCodec> codecClass =
                getOutputCompressorClass(job, Lz4Codec.class);
        codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        extension = codec.getDefaultExtension();

        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return new DataOutputStream(codec.createOutputStream(fileOut));
    }

    public static void setClassConf(Class<?> clazz, Configuration conf) {
        HadoopUtils.setClassConf(conf, CLASS_CONF_KEY, clazz);
    }

    private void setTypeRef(Configuration conf) {
        String className = conf.get(CLASS_CONF_KEY);

        if (className == null) {
            throw new RuntimeException(CLASS_CONF_KEY + " is not set");
        }

        Class<?> clazz = null;
        try {
            clazz = conf.getClassByName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("failed to instantiate class '" + className + "'", e);
        }

        typeRef = new TypeRef<M>(clazz) {
        };
    }

}
