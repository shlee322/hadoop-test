package AssociationRule;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.util.ToolRunner.run;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 10. 13.
 * Time: 오후 2:44
 * To change this template use File | Settings | File Templates.
 */
public class AssociationRule {
    private static List<String> input = new ArrayList<String>();
    private static String output;
    private static String inputDelimiter="";
    private static String outputDelimiter="";
    private static int num = 2;
    private static String host = "127.0.0.1";

    public static List<String> getInput() {
        return input;
    }

    public static String getOutput() {
        return output;
    }

    public static String getInputDelimiter()
    {
        return inputDelimiter;
    }

    public static String getOutputDelimiter()
    {
        return outputDelimiter;
    }

    public static int getNum()
    {
        return num;
    }

    public static String getHost()
    {
        return host;
    }

    public static void main(String[] args) throws Exception {
        parseArguements(args);

        //여기에 Group By 해주자.
        //그다음에 상품만 남도록 구매자 인덱스 삭제

        Mongo mongo = new Mongo(getHost());
        mongo.dropDatabase("association_rule");
        DB db = mongo.getDB("association_rule");
        db.createCollection("sanghyuck", new BasicDBObject());
        db.getCollection("sanghyuck").ensureIndex("name");
        mongo.close();

        SupportDriver supportDriver = new SupportDriver();
        run(supportDriver, null);

        AssociationRuleDriver associationRuleDriver = new AssociationRuleDriver();
        associationRuleDriver.getConfiguration().setLong("RowCount", supportDriver.getCounters().getGroup("Count").findCounter("RowCount").getValue());
        run(associationRuleDriver, null);

        //여기에 Sort, TopK
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
            } else if ("-num".equals(args[i])){
                num = Integer.parseInt(args[++i]);
            } else if ("-host".equals(args[i])){
                host = args[++i];
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
