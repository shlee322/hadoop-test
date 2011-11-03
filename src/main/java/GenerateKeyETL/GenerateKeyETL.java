package GenerateKeyETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;

import static org.apache.hadoop.util.ToolRunner.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 9. 30.
 * Time: 오후 7:31
 * To change this template use File | Settings | File Templates.
 */
public class GenerateKeyETL {
    private static List<String> input = new ArrayList<String>();
    private static String output;
    private static long start = 0;
    private static String inputDelimiter="";
    private static String outputDelimiter="";
    private static int column;
    private static String date;

    public static List<String> getInput() {
        return input;
    }

    public static String getOutput() {
        return output;
    }

    public static void main(String[] args) throws Exception {
        parseArguements(args);


        GenerateKeyDriver generateKeyDriver = new GenerateKeyDriver();

        if (date == null) {
            //key가 날짜가 아니면 줄수를 우선적으로 계산함.
            RowCountDriver rowCountDriver = new RowCountDriver();
            run(rowCountDriver, null);
            setMapperStartConf(
                    rowCountDriver.getCounters().getGroup("mapper_count"),
                    generateKeyDriver.getConfiguration());
        } else {
            String dateformat = new SimpleDateFormat(date).format(new Date());
            generateKeyDriver.getConfiguration().set("date", dateformat); //FIXME date 추가
        }

        setDelimiter(generateKeyDriver.getConfiguration());

        run(generateKeyDriver, null);
    }

    private static void setDelimiter(Configuration configuration) {
        configuration.set("inputDelimiter", inputDelimiter);
        configuration.set("outputDelimiter", outputDelimiter);
        configuration.setInt("column", column);
    }

    private static void setMapperStartConf(CounterGroup group, Configuration conf) {
        Iterator<Counter> counter = group.iterator();
        long index = start;
        while (counter.hasNext()) {
            Counter c = counter.next();
            conf.setLong(String.format("mapper_start_%s", c.getName()), index);
            index += c.getValue();
        }
    }

    private static void parseArguements(String[] args) throws IOException {
        for (int i = 0; i < args.length; ++i) {
            if ("-input".equals(args[i])) {
                input.add(args[++i]);
            } else if ("-output".equals(args[i])) {
                output = args[++i];
            } else if ("-start".equals(args[i])) {
                start = Long.parseLong(args[++i]);
            } else if ("-input_delimiter".equals(args[i])) {
                inputDelimiter = args[++i];
            } else if ("-output_delimiter".equals(args[i])) {
                outputDelimiter = args[++i];
            } else if ("-column".equals(args[i])) {
                column = Integer.parseInt(args[++i]);
            } else if ("-date".equals(args[i])){
                date = args[++i];
            }
        }

        if(input.size()==0)
        {
            System.out.println("-input 인자가 존재하지 않습니다.");
            System.exit(1);
        }

        if(output == null)
        {
            System.out.println("-output 인자가 존재하지 않습니다.");
            System.exit(1);
        }
    }
}