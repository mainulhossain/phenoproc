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

    public class WholeFileRecordReader implements RecordReader<BytesWritable, BytesWritable>
    {
        private FileSplit split;
        private JobConf conf;
        private boolean processed = false;

        private static String getFullPath(Path path)
	{
		if (path.isRoot()) {
			return path.getName();
		}
		else {
			String p = getFullPath(path.getParent()) + Path.SEPARATOR;
			return p + path.getName();
		}
	}

	public WholeFileRecordReader(FileSplit split, JobConf conf)
        {
            this.split = split;
            this.conf = conf;
        }

        @Override
        public BytesWritable createKey()
        {
            return new BytesWritable();
        }

        @Override
        public BytesWritable createValue()
        {
            return new BytesWritable();
        }

        @Override
        public boolean next(BytesWritable key, BytesWritable value) throws IOException
        {
            if (processed)
            {
                return false;
            }

            byte[] contents = new byte[(int) split.getLength()];
            Path file = split.getPath();
            String name = file.getName();
            //String name = getFullPath(file);
            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
            key.set(bytes, 0, bytes.length);
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try
            {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
            }
            finally
            {
                IOUtils.closeStream(in);
            }

            processed = true;
            return true;
        }

        @Override
        public float getProgress() throws IOException
        {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public long getPos() throws IOException
        {
            return processed ? 0l : split.getLength();
        }

        @Override
        public void close() throws IOException
        {
            // do nothing
        }
    }
