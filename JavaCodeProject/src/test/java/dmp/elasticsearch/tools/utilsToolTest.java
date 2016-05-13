package dmp.elasticsearch.tools;

import com.morty.java.dmp.elasticsearch.utilsTool;
import junit.framework.TestCase;

/**
 * Created by Administrator on 2016/05/13.
 */
public class utilsToolTest  extends TestCase{
    public void testmutiArgs(){
      new utilsTool().mutiArgs("hello","word");
    }
}
