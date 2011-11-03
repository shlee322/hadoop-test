package AssociationRule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * Date: 11. 10. 16.
 * Time: 오후 6:38
 * To change this template use File | Settings | File Templates.
 */
public class SupportDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {
    private Job job;

    public SupportDriver() throws IOException {
        job = new Job();
    }

    public int run(String[] args) throws Exception {
        for (String p : AssociationRule.getInput())
            FileInputFormat.addInputPaths(job, p);
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s_temp", AssociationRule.getOutput())));

        Configuration conf = job.getConfiguration();
        conf.set("inputDelimiter", AssociationRule.getInputDelimiter());
        conf.set("outputDelimiter", AssociationRule.getOutputDelimiter());
        conf.setInt("num", AssociationRule.getNum());
        conf.set("host",AssociationRule.getHost());

        job.setJarByClass(SupportDriver.class);
        job.setMapperClass(SupportMapper.class);
        job.setReducerClass(SupportReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Counters getCounters() throws IOException {
        return (job != null ? job.getCounters() : null);
    }
}
