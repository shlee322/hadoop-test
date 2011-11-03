package GenerateKeyETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 9. 30.
 * Time: 오후 7:49
 * To change this template use File | Settings | File Templates.
 */
public class GenerateKeyDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {
    private Job job;

    public GenerateKeyDriver() throws IOException {
        job = new Job();
    }

    public int run(String[] args) throws Exception {
        for (String p : GenerateKeyETL.getInput())
            FileInputFormat.addInputPaths(job, p);
        FileOutputFormat.setOutputPath(job, new Path(GenerateKeyETL.getOutput()));

        job.setJarByClass(GenerateKeyDriver.class);
        job.setMapperClass(GenerateKeyMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Configuration getConfiguration() {
        return job != null ? job.getConfiguration() : null;
    }
}
