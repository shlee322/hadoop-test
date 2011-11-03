package Rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 10. 9.
 * Time: 오후 6:04
 * To change this template use File | Settings | File Templates.
 */
public class RankMapper  extends Mapper<LongWritable, Text, Text, Text> {
    private int group;
    protected String inputDelimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        group = conf.getInt("group", 0);
        inputDelimiter = conf.get("inputDelimiter");
    }

    protected void map(LongWritable key, org.apache.hadoop.io.Text value, Context context) throws IOException, InterruptedException {
        String[] src = safeArray(split(value.toString()));
        context.write(new Text(src[group]), value);
    }

    protected String[] split(String s) {
        String[] r = s.split(inputDelimiter);
        if (s.substring(s.length() - inputDelimiter.length()).equals(inputDelimiter)) {
            String[] temp = new String[r.length + 1];
            System.arraycopy(r, 0, temp, 0, r.length);
            temp[r.length] = "";
            return temp;
        }
        return r;
    }

    protected String[] safeArray(String[] src)
    {
        if(src.length >= group + 1)
            return src;

        String[] temp = new String[group];
        System.arraycopy(src, 0, temp, 0, src.length);
        for(int i=src.length; i<temp.length; i++)
            temp[i] = "";
        return temp;
    }
}