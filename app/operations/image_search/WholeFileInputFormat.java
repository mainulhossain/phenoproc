import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class WholeFileInputFormat extends FileInputFormat<BytesWritable, BytesWritable>
{
    @Override
    protected boolean isSplitable(FileSystem fs, Path file)
    {
        return false;
    }

    @Override
    public RecordReader<BytesWritable, BytesWritable> getRecordReader(InputSplit split,
                                                                      JobConf conf,
                                                                      Reporter reporter)
    throws IOException
    {
        return new WholeFileRecordReader((FileSplit) split, conf);
    }
}
