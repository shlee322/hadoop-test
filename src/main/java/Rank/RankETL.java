package Rank;

import GenerateKeyETL.GenerateKeyETL;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.util.ToolRunner.run;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 10. 9.
 * Time: 오후 3:22
 * To change this template use File | Settings | File Templates.
 */
public class RankETL {
    private static List<String> input = new ArrayList<String>();
    private static String output;
    private static String inputDelimiter;
    private static String outputDelimiter;
    private static int group;
    private static int top;
    private static int column;

    public static List<String> getInput() {
        return input;
    }

    public static String getOutput() {
        return output;
    }

    public static void main(String[] args) throws Exception {
        parseArguements(args);

        GenerateKeyETL.main(generatekeyArgs());

        input.clear();
        input.add(output + "_ranktemp");

        RankDriver rankDriver = new RankDriver();

        Configuration conf = rankDriver.getConfiguration();
        conf.set("inputDelimiter", inputDelimiter);
        conf.set("outputDelimiter", outputDelimiter);
        conf.setInt("group", group + 1);
        conf.setInt("top", top);
        conf.setInt("column", column);

        run(rankDriver, null);
    }

    private static String[] generatekeyArgs()
    {
        List<String> args = new ArrayList<String>();

        for(String path : input)
        {
            args.add("-input");
            args.add(path);
        }

        args.add("-output");
        args.add(output + "_ranktemp");

        args.add("-input_delimiter");
        args.add(inputDelimiter);

        args.add("-output_delimiter");
        args.add(inputDelimiter);

        args.add("-column");
        args.add("0");

        return args.toArray(new String[]{});
    }

    private static void parseArguements(String[] args) throws IOException {
        for (int i = 0; i < args.length; ++i) {
            if ("-input".equals(args[i])) {
                input.add(args[++i]);
            } else if ("-output".equals(args[i])) {
                output = args[++i];
            } else if ("-input_delimiter".equals(args[i])) {
                inputDelimiter = args[++i];
            } else if ("-output_delimiter".equals(args[i])) {
                outputDelimiter = args[++i];
            } else if ("-group".equals(args[i])) {
                group = Integer.parseInt(args[++i]);
            } else if ("-top".equals(args[i])) {
                top = Integer.parseInt(args[++i]);
            } else if ("-column".equals(args[i])) {
                column = Integer.parseInt(args[++i]);
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
