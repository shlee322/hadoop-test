package GenerateKeyETL;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 9. 30.
 * Time: 오후 7:32
 * To change this template use File | Settings | File Templates.
 */
public class RowCountDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {
    private Job job;

    public int run(String[] args) throws Exception {
        job = new Job();

        for (String p : GenerateKeyETL.getInput())
            FileInputFormat.addInputPaths(job, p);
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s_temp", GenerateKeyETL.getOutput())));

        job.setJarByClass(RowCountDriver.class);
        job.setMapperClass(RowCountMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Counters getCounters() throws IOException {
        return (job != null ? job.getCounters() : null);
    }
}
