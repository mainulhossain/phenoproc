import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class NamedFileOutputFormat extends MultipleOutputFormat<BytesWritable, BytesWritable>
{
    @Override
    protected String generateFileNameForKeyValue(BytesWritable key, BytesWritable value, String name)
    {
        return new String(key.getBytes(), 0, key.getLength(), StandardCharsets.UTF_8);
    }

    @Override
    public RecordWriter<BytesWritable, BytesWritable> getBaseRecordWriter(FileSystem ignored,
                                                                          JobConf conf,
                                                                          String name,
                                                                          Progressable progress)
    throws IOException
    {
        Path file = FileOutputFormat.getTaskOutputPath(conf, name);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream out = fs.create(file, progress);
        return new BytesValueWriter(out);
    }
}
