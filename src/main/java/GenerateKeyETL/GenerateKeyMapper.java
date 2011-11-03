package GenerateKeyETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.sql.rowset.Joinable;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Array;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 9. 30.
 * Time: 오후 7:54
 * To change this template use File | Settings | File Templates.
 */
public class GenerateKeyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    protected long index;
    protected String date;
    protected String inputDelimiter;
    protected String outputDelimiter;
    protected int column;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        date = conf.get("date");
        if (date == null)
            index = conf.getLong(String.format("mapper_start_%s", conf.get("mapred.task.partition", "-1")), 0);
        inputDelimiter = conf.get("inputDelimiter");
        outputDelimiter = conf.get("outputDelimiter");
        column = conf.getInt("column", 0);
    }

    protected void map(LongWritable key, org.apache.hadoop.io.Text value, Context context) throws IOException, InterruptedException {
        String[] src = safeArray(split(value.toString()));
        StringBuffer res = new StringBuffer();

        //key가 들어갈 컬럼전까지
        append(src, res, 0, column);

        //key 컬럼
        res.append(date == null ? String.valueOf(index++) : date);
        res.append(outputDelimiter);

        //key 컬럼 이후 값
        append(src, res, column, src.length);

        context.write(NullWritable.get(), new Text(res.substring(0, res.length() - outputDelimiter.length())));
    }

    //문자열 맨뒤에 구분자가 올 경우, 배열의 맨뒤에 빈칸처리
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

    protected void append(String[] src, StringBuffer res, int start, int end)
    {
        for(int i=start; i<end; i++)
        {
            res.append(src[i]);
            res.append(outputDelimiter);
        }
    }

    //만약 컬럼 인덱스가 문자열 배열의 길이보다 클 경우를 위하여.
    protected String[] safeArray(String[] src)
    {
        if(src.length >= column + 1)
            return src;

        String[] temp = new String[column];
        System.arraycopy(src, 0, temp, 0, src.length);
        for(int i=src.length; i<temp.length; i++)
            temp[i] = "";
        return temp;
    }
}

