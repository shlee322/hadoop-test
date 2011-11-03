import GenerateKeyETL.GenerateKeyMapper;

/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 10. 9.
 * Time: 오후 2:55
 * To change this template use File | Settings | File Templates.
 */
public class GenerateKeyMapperTest extends GenerateKeyMapper {
    static public void main(String[] args)
    {
        GenerateKeyMapperTest Test = new GenerateKeyMapperTest();


        Test.splitTest("테스트,입니다,히히히",",","`");
        Test.splitTest("테스트,입니다,히히히,",",","`");
        Test.splitTest("테스트,입니다,히히히",",","`");
        //Test.splitTest("",",","`"); //나중에수정하자.
        Test.splitTest("테스트,입니다","","`");

        Test.appendTest("테스트,입니다,히히히",0);
        Test.appendTest("테스트,입니다,히히히",1);
        //Test.appendTest("테스트,입니다,히히히",4); 차후예외처
        Test.appendTest("테스트,입니다,히히히",3);

    }

    public void appendTest(String s, int column)
    {
        String[] src = splitTest(s,",",",");
        StringBuffer res = new StringBuffer();

        append(src, res, 0, column);

        res.append(123);
        res.append(outputDelimiter);

        append(src, res, column, src.length);
        System.out.println(res.toString());
    }

    public String[] splitTest(String s, String _inputDelimiter, String _outputDelimiter)
    {
        inputDelimiter = _inputDelimiter;
        outputDelimiter = _outputDelimiter;
        String[] r = split(s);
        for(String t : r)
        {
            System.out.print(t);
            System.out.print(outputDelimiter);
        }
        System.out.println();
        return r;
    }
}
