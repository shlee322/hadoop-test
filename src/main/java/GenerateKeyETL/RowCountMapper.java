package GenerateKeyETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 9. 30.
 * Time: 오후 7:41
 * To change this template use File | Settings | File Templates.
 */
public class RowCountMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
    private long count;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.count = 0;
    }

    protected void map(LongWritable key, org.apache.hadoop.io.Text value, Context context) throws IOException, InterruptedException {
        ++this.count;
    }

    protected void cleanup(Context context
    ) throws IOException, InterruptedException {
        context.getCounter("mapper_count",
                context.getConfiguration().get("mapred.task.partition", "0"))
                .increment(this.count);
    }
}
