package AssociationRule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 10. 13.
 * Time: 오후 2:45
 * To change this template use File | Settings | File Templates.
 */
public class AssociationRuleMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private String inputDelimiter;
    private String outputDelimiter;
    private int num;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        inputDelimiter = conf.get("inputDelimiter");
        outputDelimiter = conf.get("outputDelimiter");
        num = conf.getInt("num", 2);
    }

    protected void map(LongWritable key, org.apache.hadoop.io.Text value, Context context) throws IOException, InterruptedException {

        int max = num;
        String[] text = value.toString().split(inputDelimiter);

        if(max>text.length)
            max = text.length;

        for(int i=2; i<=max; i++)
            Permutations(context, text, i);
    }

    protected void Permutations(Context context, String[] value, int n) throws IOException, InterruptedException
    {
        try{
            Permutations permutations = new Permutations(value, n);
            while(permutations.hasMoreElements())
            {
                Object[] element = (Object[])permutations.nextElement();
                StringBuffer buffer = new StringBuffer();

                for(Object string : element)
                    buffer.append((String)string + outputDelimiter);

                context.write(new Text(buffer.toString().substring(0, buffer.toString().length() - outputDelimiter.length())), NullWritable.get());
            }
        }catch (CombinatoricException e)
        {
            e.printStackTrace();
        }
    }
}