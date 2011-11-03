/**
 * Created by IntelliJ IDEA.
 * User: shlee322
 * Date: 11. 10. 16.
 * Time: 오후 6:05
 * To change this template use File | Settings | File Templates.
 */
public class Permutation {
    static public void main(String[] args)
    {
        String []test = new String[]{"a","b","c","d"};
        for(int i=0; i<test.length; i++)
        {
            System.out.print(test[i]);
            for(int ii=1; ii<test.length; ii++)
            {
                System.out.print(test[ii]);
                System.out.println();
                /*for(int iii=2; iii<test.length; iii++)
                {
                    System.out.print(test[iii]);
                    System.out.println();
                }*/
            }
        }
    }
}
