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

public class BytesValueWriter implements RecordWriter<BytesWritable, BytesWritable>
    {
        FSDataOutputStream out;

        BytesValueWriter(FSDataOutputStream out)
        {
            this.out = out;
        }

        @Override
        public synchronized void write(BytesWritable key, BytesWritable value) throws IOException
        {
            out.write(key.getBytes(), 0, key.getLength());
	    String t = "\t";
	    byte[] bytes = t.getBytes(StandardCharsets.UTF_8);	    
            out.write(bytes, 0, bytes.length);
            out.write(value.getBytes(), 0, value.getLength());
	    t = "\n";
	    bytes = t.getBytes(StandardCharsets.UTF_8);	    
            out.write(bytes, 0, bytes.length);
        }

        @Override
        public void close(Reporter reporter) throws IOException
        {
            out.close();
        }
    }
