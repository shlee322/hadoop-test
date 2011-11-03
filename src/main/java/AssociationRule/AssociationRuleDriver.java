package AssociationRule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 10. 13.
 * Time: 오후 3:11
 * To change this template use File | Settings | File Templates.
 */
public class AssociationRuleDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {
    private Job job;

    public AssociationRuleDriver() throws IOException {
        job = new Job();
    }

    public int run(String[] args) throws Exception {
        for (String p : AssociationRule.getInput())
            FileInputFormat.addInputPaths(job, p);
        FileOutputFormat.setOutputPath(job, new Path(AssociationRule.getOutput()));

        Configuration conf = job.getConfiguration();
        conf.set("inputDelimiter", AssociationRule.getInputDelimiter());
        conf.set("outputDelimiter", AssociationRule.getOutputDelimiter());
        conf.setInt("num",AssociationRule.getNum() + 1);
        conf.set("host",AssociationRule.getHost());

        job.setJarByClass(AssociationRuleDriver.class);
        job.setMapperClass(AssociationRuleMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(AssociationRuleReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Configuration getConfiguration() {
        return job != null ? job.getConfiguration() : null;
    }
}
